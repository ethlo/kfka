package com.ethlo.kfka.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.util.AbstractIterator;
import com.ethlo.kfka.util.CloseableIterator;

/*-
 * #%L
 * kfka-core
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

public class MemoryKfkaMessageStore implements KfkaMessageStore
{
    private final AtomicLong counter = new AtomicLong(1);
    private final ConcurrentMap<Long, KfkaMessage> map = new ConcurrentSkipListMap<>();

    @Override
    public <T extends KfkaMessage> void add(final T message)
    {
        final long newId = counter.getAndIncrement();
        message.setId(newId);
        map.put(message.getId(), message);
    }

    @Override
    public <T extends KfkaMessage> CloseableIterator<T> tail()
    {
        final Iterator<KfkaMessage> src = map.values().iterator();
        return new AbstractIterator<T>()
        {
            @Override
            public void close()
            {

            }

            @Override
            protected T computeNext()
            {
                return src.hasNext() ? (T) src.next() : endOfData();
            }
        };
    }

    @Override
    public <T extends KfkaMessage> CloseableIterator<T> head()
    {
        return null;
    }

    @Override
    public <T extends KfkaMessage> void addAll(final Collection<T> data)
    {
        data.forEach(msg -> map.put(msg.getId(), msg));
    }

    @Override
    public long size()
    {
        return map.size();
    }

    @Override
    public void clear()
    {
        counter.set(1);
        map.clear();
    }

    @Override
    public void sendAfter(final long messageId, final KfkaMessageListener l)
    {
        for (Map.Entry<Long, KfkaMessage> e : map.entrySet())
        {
            if (e.getKey() >= messageId)
            {
                l.onMessage(e.getValue());
            }
        }
    }

    @Override
    public Optional<Long> getOffsetMessageId(final int offset)
    {
        final int abs = Math.abs(offset);
        final List<KfkaMessage> values = new ArrayList<>(map.values());
        Collections.reverse(values);
        final Iterator<KfkaMessage> iter = values.iterator();
        int count = 0;
        Long id = null;
        while (iter.hasNext() && count++ < abs)
        {
            id = iter.next().getId();
        }
        return Optional.ofNullable(id);
    }
}