package com.ethlo.kfka;

import java.util.Comparator;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

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
import com.hazelcast.util.IterationType;

public class KfkaManagerImpl implements KfkaManager
{
    private final static Logger logger = LoggerFactory.getLogger(KfkaManagerImpl.class);
    
    private Map<KfkaMessageListener, KfkaPredicate> msgListeners = new IdentityHashMap<>();
    private IMap<Long, KfkaMessage> messages;
    private IAtomicLong counter;
    private KfkaConfig kfkaCfg;
    private KfkaMapStore<? extends KfkaMessage> mapStore;
    
    public KfkaManagerImpl(HazelcastInstance hazelcastInstance, KfkaMapStore<? extends KfkaMessage> mapStore, KfkaCounterStore counterStore, KfkaConfig kfkaCfg)
    {
        this.kfkaCfg = kfkaCfg;
        
        final MapConfig hzcfg = hazelcastInstance.getConfig().getMapConfig(kfkaCfg.getName());
        hzcfg.setEvictionPolicy(EvictionPolicy.NONE);
        final MapStoreConfig mapCfg = hzcfg.getMapStoreConfig();
        mapCfg.setImplementation(mapStore);
        mapCfg.setEnabled(true);
        mapCfg.setWriteBatchSize(kfkaCfg.getBatchSize());
        mapCfg.setWriteDelaySeconds(kfkaCfg.getWriteDelay());
        mapCfg.setInitialLoadMode(InitialLoadMode.EAGER);
        
        this.mapStore = mapStore;
        this.messages = hazelcastInstance.getMap(kfkaCfg.getName());
        this.counter = hazelcastInstance.getAtomicLong(kfkaCfg.getName());
        
        messages.addIndex("id", true);
        messages.addIndex("timestamp", true);
        
        messages.addEntryListener(new EntryAddedListener<Long, KfkaMessage>()
        {
            @Override
            public void entryAdded(EntryEvent<Long, KfkaMessage> event)
            {
                for (Entry<KfkaMessageListener, KfkaPredicate> e : msgListeners.entrySet())
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
            logger.info("Setting current KFKA message ID counter to {}", initialValue);
            counter.compareAndSet(0, initialValue);
        }
    }
    
    public void addListener(KfkaMessageListener l)
    {
        this.msgListeners.put(l, new KfkaPredicate(this));
    }
    
    @Override
    public void add(KfkaMessage msg)
    {
        final long id = counter.incrementAndGet();
        msg.id(id);
        this.messages.put(id, msg, kfkaCfg.getTtl(TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
    
    @Override
    @Scheduled(fixedRate=60_000)
    public void clean()
    {
        final long oldest = System.currentTimeMillis() - kfkaCfg.getTtl(TimeUnit.MILLISECONDS);
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

    public KfkaMessageListener addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate)
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
        
        pagingPredicate.setIterationType(IterationType.VALUE);
        
        //messages.entrySet().stream().forEach(e->{System.err.println(e);});
            
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
        msgListeners.put(l, kfkaPredicate);
        
        return l;
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
        final Iterator<List<Long>> iter = Iterators.partition(mapStore.loadAllKeys().iterator(), kfkaCfg.getBatchSize());
        while (iter.hasNext())
        {
            messages.getAll(new TreeSet<>(iter.next()));
        }
        return messages.size();
    }

    @Override
    public void removeListener(KfkaMessageListener listener)
    {
        this.msgListeners.remove(listener);
    }
}
