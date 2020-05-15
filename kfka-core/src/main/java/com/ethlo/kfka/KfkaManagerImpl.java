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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.kfka.persistence.KfkaCounterStore;
import com.ethlo.kfka.persistence.KfkaMapStore;
import com.ethlo.kfka.util.PartitionedIterator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;

public class KfkaManagerImpl implements KfkaManager
{
    private static final Logger logger = LoggerFactory.getLogger(KfkaManagerImpl.class);
    private static final Comparator<Entry<Long, KfkaMessage>> ORDER_BY_ID_DESCENDING = (a, b) -> b.getValue().getId().compareTo(a.getValue().getId());
    private final Map<KfkaMessageListener, KfkaPredicate> msgListeners = new ConcurrentHashMap<>();
    private final IMap<Long, KfkaMessage> messages;
    private final PNCounter counter;
    private final KfkaConfig kfkaCfg;
    private final KfkaMapStore<? extends KfkaMessage> mapStore;

    public KfkaManagerImpl(HazelcastInstance hazelcastInstance, KfkaMapStore<? extends KfkaMessage> mapStore, KfkaCounterStore counterStore, KfkaConfig kfkaCfg)
    {
        this.kfkaCfg = kfkaCfg;

        final MapConfig hzcfg = hazelcastInstance.getConfig().getMapConfig(kfkaCfg.getName());
        hzcfg.setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.NONE));

        final MapStoreConfig mapCfg = hzcfg.getMapStoreConfig();
        mapCfg.setImplementation(mapStore);
        mapCfg.setEnabled(kfkaCfg.isPersistent());
        mapCfg.setWriteBatchSize(kfkaCfg.getBatchSize());
        mapCfg.setWriteDelaySeconds(kfkaCfg.getWriteDelay());
        mapCfg.setInitialLoadMode(kfkaCfg.getInitialLoadMode());

        this.mapStore = mapStore;

        this.messages = hazelcastInstance.getMap(kfkaCfg.getName());
        this.counter = hazelcastInstance.getPNCounter(kfkaCfg.getName());

        messages.addIndex(IndexType.SORTED, "id");
        messages.addIndex(IndexType.SORTED, "timestamp");

        messages.addEntryListener((EntryAddedListener<Long, KfkaMessage>) event ->
        {
            logger.debug("Received message for dispatch: {}", event.getValue());
            for (final Entry<KfkaMessageListener, KfkaPredicate> e : msgListeners.entrySet())
            {
                final KfkaPredicate predicate = e.getValue();
                final KfkaMessage msg = event.getValue();

                // Check if message should be included
                if (predicate.toPredicate().test(msg))
                {
                    final KfkaMessageListener l = e.getKey();
                    logger.debug("Sending message {} to {}", event.getValue().getId(), e.getKey());
                    l.onMessage(event.getValue());
                }
            }
        }, true);

        if (counter.get() == 0)
        {
            final long initialValue = counterStore.latest();
            logger.info("Setting current KFKA message ID counter to {}", initialValue);
            counter.addAndGet(initialValue);
        }
    }

    @Override
    public void addListener(KfkaMessageListener l)
    {
        this.addListener(l, new KfkaPredicate());
    }

    @Override
    public long add(KfkaMessage msg)
    {
        final long id = counter.incrementAndGet();
        msg.id(id);
        if (msg.getTimestamp() == null)
        {
            msg.timestamp(System.currentTimeMillis());
        }
        this.messages.put(id, msg, kfkaCfg.getTtl().toMillis() / 1000, TimeUnit.SECONDS);
        return id;
    }

    @Override
    public long size()
    {
        return messages.size();
    }

    @Override
    public long findfirst()
    {
        return messages.aggregate(new MinAggregator<>("id"));
    }

    @Override
    public long findLatest()
    {
        return messages.aggregate(new MaxAggregator<>("id"));
    }

    @Override
    public void clearAll()
    {
        this.messages.clear();
        this.counter.reset();
    }

    public KfkaMessageListener addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate)
    {
        // Relative offset
        final Integer offset = kfkaPredicate.getRelativeOffset();
        if (offset == null && kfkaPredicate.getMessageId() != null)
        {
            // We have just a message id
            sendHistoricData(Predicates.pagingPredicate(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, kfkaCfg.getMaxQuerySize()), l);
        }
        else if (offset != null && kfkaPredicate.getMessageId() == null)
        {
            // We have just a relative offset
            sendHistoricData(Predicates.pagingPredicate(kfkaPredicate.toHazelcastPredicate(), ORDER_BY_ID_DESCENDING, -offset), l);
        }

        // Add to set of listeners, with the desired predicate
        msgListeners.put(l, kfkaPredicate);

        return l;
    }

    private void sendHistoricData(PagingPredicate<Long, KfkaMessage> pagingPredicate, KfkaMessageListener l)
    {
        // FIXME: pagingPredicate.setIterationType(IterationType.VALUE);
        final Collection<KfkaMessage> hits = messages.values(pagingPredicate);

        // Deliver all messages up until now
        hits.stream().sorted(Comparator.comparing(KfkaMessage::getId)).forEach(l::onMessage);
    }

    @Override
    public void clearCache()
    {
        this.messages.evictAll();
        logger.info("Evicted all entries");
    }

    @Override
    public long loadAll()
    {
        final Iterator<List<Long>> iter = new PartitionedIterator<>(mapStore.loadAllKeys().iterator(), kfkaCfg.getBatchSize());
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

    @Override
    public void delete(long messageId)
    {
        this.messages.remove(messageId);
    }
}
