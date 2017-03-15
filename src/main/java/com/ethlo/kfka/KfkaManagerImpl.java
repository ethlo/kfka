package com.ethlo.kfka;

/*-
 * #%L
 * kfka
 * %%
 * Copyright (C) 2017 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.ethlo.kfka.persistence.KfkaCounterStore;
import com.ethlo.kfka.persistence.KfkaMapStore;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.util.IterationType;

public class KfkaManagerImpl implements KfkaManager
{
    private final static Logger logger = LoggerFactory.getLogger(KfkaManagerImpl.class);

    private final Map<KfkaMessageListener, KfkaPredicate> msgListeners = new IdentityHashMap<>();
    private final IMap<Long, KfkaMessage> messages;
    private final IAtomicLong counter;
    private final KfkaConfig kfkaCfg;
    private final KfkaMapStore<? extends KfkaMessage> mapStore;
    private final CleanProcessor cleanProcessor = new CleanProcessor();
    
    public KfkaManagerImpl(HazelcastInstance hazelcastInstance, KfkaMapStore<? extends KfkaMessage> mapStore, KfkaCounterStore counterStore, KfkaConfig kfkaCfg)
    {
        this.kfkaCfg = kfkaCfg;
        
        final MapConfig hzcfg = hazelcastInstance.getConfig().getMapConfig(kfkaCfg.getName());
        hzcfg.setEvictionPolicy(EvictionPolicy.NONE);
        
        final MapStoreConfig mapCfg = hzcfg.getMapStoreConfig();
        mapCfg.setImplementation(mapStore);
        mapCfg.setEnabled(kfkaCfg.isPersistent());
        mapCfg.setWriteBatchSize(kfkaCfg.getBatchSize());
        mapCfg.setWriteDelaySeconds(kfkaCfg.getWriteDelay());
        mapCfg.setInitialLoadMode(kfkaCfg.getInitialLoadMode());
        
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
        this.addListener(l, new KfkaPredicate());
    }
    
    @Override
    public void add(KfkaMessage msg)
    {
        final long id = counter.incrementAndGet();
        msg.id(id);
        this.messages.put(id, msg, kfkaCfg.getTtl(TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
    
    @Override
    @Scheduled(fixedRate=180_000)
    public void clean()
    {
        final long oldest = System.currentTimeMillis() - kfkaCfg.getTtl(TimeUnit.MILLISECONDS);
        final Predicate<?, ?> p = Predicates.lessThan("timestamp", oldest); 
        this.messages.executeOnEntries(cleanProcessor, p);
    }

    @Override
    public long findfirst(String topic, String type)
    {
        return doFind(true, topic);
    }
    
    private long doFind(boolean first, String topic)
    {
        return messages.aggregate(first ? Aggregators.longMin() : Aggregators.longMax());
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
        // Relative offset
        final Integer offset = kfkaPredicate.getRelativeOffset();
        if (offset == null && kfkaPredicate.getMessageId() != null)
        {
            // We have just a message id
            sendHistoricData(new PagingPredicate<Long, KfkaMessage>(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, kfkaCfg.getMaxQuerySize()), l);
        }
        else if (offset != null && kfkaPredicate.getMessageId() == null)
        {
            // We have just a relative offset
            sendHistoricData(new PagingPredicate<Long, KfkaMessage>(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, -offset), l);
        }
        
        // Add to set of listeners, with the desired predicate
        msgListeners.put(l, kfkaPredicate);
        
        return l;
    }
    
    private void sendHistoricData(PagingPredicate<Long, KfkaMessage> pagingPredicate, KfkaMessageListener l)
    {
        pagingPredicate.setIterationType(IterationType.VALUE);
        final Collection<KfkaMessage> hits = messages.values(pagingPredicate);
        
        // Deliver all messages up until now
        FluentIterable.from(hits)
            .toSortedList(new Comparator<KfkaMessage>()
            {
                @Override
                public int compare(KfkaMessage a, KfkaMessage b)
                {
                    return a.getId().compareTo(b.getId());
                }
            })
            .forEach(e -> l.onMessage(e));
    }

    private static final Comparator<Entry<Long, KfkaMessage>> ORDER_BY_ID_DESCENDING = new SerializableComparator<Entry<Long, KfkaMessage>>()
    {
        private static final long serialVersionUID = 6647415692489347533L;

        @Override
        public int compare(Entry<Long, KfkaMessage> a, Entry<Long, KfkaMessage> b)
        {
            return b.getValue().getId().compareTo(a.getValue().getId());
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
