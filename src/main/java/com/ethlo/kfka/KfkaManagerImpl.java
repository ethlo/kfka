package com.ethlo.kfka;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.google.common.collect.FluentIterable;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
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
    
    public KfkaManagerImpl(HazelcastInstance hazelcastInstance, KfkaMapStore mapStore, KfkaCounterStore counterStore)
    {
        final MapConfig cfg = hazelcastInstance.getConfig().getMapConfig("kfka");
        cfg.setEvictionPolicy(EvictionPolicy.NONE);
        cfg.getMapStoreConfig().setImplementation(mapStore);
        cfg.getMapStoreConfig().setEnabled(true);
        cfg.getMapStoreConfig().setWriteBatchSize(500);
        cfg.getMapStoreConfig().setWriteDelaySeconds(0);
        
        this.messages = hazelcastInstance.getMap("kfka");
        this.counter = hazelcastInstance.getAtomicLong("kfka");
        
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
    }
}
