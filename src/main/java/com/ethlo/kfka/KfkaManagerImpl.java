package com.ethlo.kfka;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.acme.CustomKfkaMessage;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

public class KfkaManagerImpl implements KfkaManager
{
    private final static Logger logger = LoggerFactory.getLogger(KfkaManagerImpl.class);
    
    private Set<Entry<KfkaMessageListener, KfkaPredicate>> msgListeners = new LinkedHashSet<>();
    private IMap<Long, KfkaMessage> messages;
    private IAtomicLong counter;
    private long ttlSeconds = 300;

    private KfkaMapStore<CustomKfkaMessage> mapStore;
    
    public KfkaManagerImpl(HazelcastInstance hazelcastInstance, KfkaMapStore<CustomKfkaMessage> mapStore, KfkaCounterStore counterStore)
    {
        final String name = "kfka";
        final MapConfig cfg = hazelcastInstance.getConfig().getMapConfig(name);
        cfg.setEvictionPolicy(EvictionPolicy.NONE);
        final MapStoreConfig mapCfg = cfg.getMapStoreConfig();
        mapCfg.setImplementation(mapStore);
        mapCfg.setEnabled(true);
        mapCfg.setWriteBatchSize(500);
        mapCfg.setWriteDelaySeconds(3);
        mapCfg.setInitialLoadMode(InitialLoadMode.EAGER);
        
        this.mapStore = mapStore;
        this.messages = hazelcastInstance.getMap(name);
        this.counter = hazelcastInstance.getAtomicLong(name);
        
        messages.addEntryListener(new EntryAddedListener<Long, KfkaMessage>()
        {
            @Override
            public void entryAdded(EntryEvent<Long, KfkaMessage> event)
            {
                for (Entry<KfkaMessageListener, KfkaPredicate> e : msgListeners)
                {
                    final KfkaPredicate predicate = e.getValue();
                    final KfkaMessage msg = event.getValue();
                    
                    // Check if message should be included
                    if (predicate.toGuavaPredicate().apply(msg))
                    {
                        final KfkaMessageListener l = e.getKey();
                        l.onMessage(event.getValue());
                    }
                }
            }
        }, true);
        
        if (counter.get() == 0)
        {
            final long initialValue = counterStore.latest();
            logger.info("Setting initial KFKA message ID counter to {}", initialValue);
            counter.compareAndSet(0, initialValue);
        }
    }
    
    public void addListener(KfkaMessageListener l)
    {
        this.msgListeners.add(new AbstractMap.SimpleEntry<>(l,  new KfkaPredicate(this)));
    }
    
    @Override
    public void add(KfkaMessage msg)
    {
        final long id = counter.incrementAndGet();
        msg.id(id);
        this.messages.put(id, msg, ttlSeconds, TimeUnit.SECONDS);
    }
    
    @Override
    @Scheduled(fixedRate=60_000)
    public void clean()
    {
        final long oldest = System.currentTimeMillis() - (ttlSeconds * 1000);
        final AtomicInteger removed = new AtomicInteger(0);
        final Predicate<?, ?> p = Predicates.lessThan("timestamp", oldest); 
        this.messages.executeOnEntries(new AbstractEntryProcessor<Long, KfkaMessage>()
        {
            private static final long serialVersionUID = 6953483674346132344L;

            @Override
            public Object process(Entry<Long, KfkaMessage> entry)
            {
                entry.setValue(null);
                removed.incrementAndGet();
                return true;
            }           
        }, p);
        
        if (removed.get() > 0)
        {
            logger.info("Removed {} expired items older than {}", removed.get(), new Date(oldest).toInstant().toString());
        }
    }

    @Override
    public long findfirst(String topic, String type)
    {
        return doFind(true, topic);
    }
    
    private long doFind(boolean first, String topic)
    {
        Supplier<Long, KfkaMessage, Long> supplier = 
            Supplier.fromPredicate((e) -> e.getValue().getTopic().equalsIgnoreCase(topic), 
            Supplier.all((KfkaMessage m) -> m.getId()));
        return messages.aggregate(supplier, first ? Aggregations.longMin() : Aggregations.longMax());
    }

    @Override
    public long findLatest(String topic, String type)
    {
        return doFind(false, topic);
    }

    @Override
    public void clearAll()
    {
        this.messages.clear();
        this.counter.set(0);
    }

    public void addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate)
    {
        // Offset
        final Integer offset = kfkaPredicate.getOffset();
        PagingPredicate pagingPredicate; 
        int skip = 0;
        if (offset == null)
        {
            pagingPredicate = new PagingPredicate(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, 1);
            skip = 1;
        }
        else
        {
            pagingPredicate = new PagingPredicate(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, -offset);
        }
            
        // Deliver all messages up until now
        FluentIterable
            .from(messages.values(pagingPredicate))
                .skip(skip)
                .toSortedList(new Comparator<KfkaMessage>()
                {
                    @Override
                    public int compare(KfkaMessage a, KfkaMessage b)
                    {
                        return a.getId().compareTo(b.getId());
                    }
                })
                .forEach(e -> l.onMessage(e));
        
        // Add to set of listeners, with the desired predicate
        this.msgListeners.add(new AbstractMap.SimpleEntry<>(l, kfkaPredicate));
    }
    
    @SuppressWarnings("rawtypes")
    private static final Comparator<Entry> ORDER_BY_ID_DESCENDING = new Comparator<Entry>()
    {
        @Override
        public int compare(Entry a, Entry b)
        {
            return ((KfkaMessage)b.getValue()).getId().compareTo(((KfkaMessage)a.getValue()).getId());
        }
    };

    @Override
    public void clearCache()
    {
        this.messages.evictAll();
        logger.info("Evicted all entries");
    }

    @Override
    public long loadAll()
    {
        final int batchSize = 500;
        final Iterator<List<Long>> iter = Iterators.partition(mapStore.loadAllKeys().iterator(), batchSize);
        while (iter.hasNext())
        {
            messages.getAll(new TreeSet<>(iter.next()));
        }
        return messages.size();
    }
}
