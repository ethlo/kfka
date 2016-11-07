package com.ethlo.kfka;

import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.common.collect.FluentIterable;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

@Service
public class KfkaManager
{
    private final static Logger logger = LoggerFactory.getLogger(KfkaManager.class);
    
    private Set<KfkaMessageListener> msgListeners = new LinkedHashSet<>();
    private IMap<Long, KfkaMessage> messages;
    private IAtomicLong counter;
    private long ttlSeconds = 300;
    
    @Autowired 
    public KfkaManager(HazelcastInstance hazelcastInstance, MapStore<Long, KfkaMessage> mapStore, KfkaCounterStore counterStore)
    {
        final MapConfig cfg = hazelcastInstance.getConfig().getMapConfig("kfka");
        cfg.setEvictionPolicy(EvictionPolicy.NONE);
        cfg.getMapStoreConfig().setImplementation(mapStore);
        cfg.getMapStoreConfig().setEnabled(true);
        cfg.getMapStoreConfig().setWriteBatchSize(500);
        cfg.getMapStoreConfig().setWriteDelaySeconds(3);
        
        this.messages = hazelcastInstance.getMap("kfka");
        this.counter = hazelcastInstance.getAtomicLong("kfka");
        
        messages.addEntryListener(new EntryAddedListener<Long, KfkaMessage>()
        {
            @Override
            public void entryAdded(EntryEvent<Long, KfkaMessage> event)
            {
                for (KfkaMessageListener l : msgListeners)
                {
                    l.onMessage(event.getValue());
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
    
    public void addListener(KfkaMessageListener l, int jumpBack)
    {
        final PagingPredicate pagingPredicate = new PagingPredicate(jumpBack);
        messages.values(pagingPredicate).forEach(e->{l.onMessage(e);});
        this.msgListeners.add(l);
    }
    
    public void add(KfkaMessage msg)
    {
        final long id = counter.incrementAndGet();
        msg.id(id);
        this.messages.put(id, msg, ttlSeconds, TimeUnit.SECONDS);
    }
    
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

    public long findLatest(String topic, String type)
    {
        return doFind(false, topic);
    }

    public void clearAll()
    {
        this.messages.clear();
        this.counter.set(0);
    }

    public void addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate)
    {
        final List<Predicate<?,?>> predicates = new LinkedList<>();
        
        // Position
        final Long offsetId = kfkaPredicate.getOffsetId();
        if (offsetId != null)
        {
            predicates.add(Predicates.greaterEqual("id", offsetId));
        }
        
        // Topic
        if (kfkaPredicate.getTopic() != null)
        {
            predicates.add(Predicates.equal("topic", kfkaPredicate.getTopic()));
        }
        
        final Predicate<?,?> all = Predicates.and(predicates.toArray(new Predicate[predicates.size()]));
        
        // Offset
        final Integer offset = kfkaPredicate.getOffset();
        PagingPredicate pagingPredicate; 
        int skip = 0;
        if (offset == null)
        {
            pagingPredicate = new PagingPredicate(all, ORDER_BY_ID, 1);
            skip = 1;
        }
        else
        {
            pagingPredicate = new PagingPredicate(all, ORDER_BY_ID, -offset);
        }
            
        // Deliver all messages up until now
        FluentIterable.from(messages.values(pagingPredicate)).skip(skip).forEach(e -> l.onMessage(e));
        
    }
    
    @SuppressWarnings("rawtypes")
    private static final Comparator<Entry> ORDER_BY_ID = new Comparator<Entry>()
    {
        @Override
        public int compare(Entry a, Entry b)
        {
            return ((KfkaMessage)a.getValue()).getId().compareTo(((KfkaMessage)b.getValue()).getId());
        }
    };
}
