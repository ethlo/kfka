package com.ethlo.kfka.persistence;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.acme.CustomKfkaMessage;

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

public class MemoryKfkaMapStore<T> implements KfkaMapStore<CustomKfkaMessage>
{
    private final ConcurrentMap<Long, MetadataAwareValue<CustomKfkaMessage>> map = new ConcurrentHashMap<>();
    private final Duration ttl;

    public MemoryKfkaMapStore(Duration ttl)
    {
        this.ttl = ttl;
    }

    @Override
    public void store(Long key, MetadataAwareValue<CustomKfkaMessage> value)
    {
        map.put(key, value);
    }

    @Override
    public void storeAll(Map<Long, MetadataAwareValue<CustomKfkaMessage>> map)
    {
        this.map.putAll(map);
    }

    @Override
    public void delete(Long key)
    {
        map.remove(key);
    }

    @Override
    public void deleteAll(Collection<Long> keys)
    {
        keys.forEach(map::remove);
    }

    @Override
    public MetadataAwareValue<CustomKfkaMessage> load(Long key)
    {
        return map.get(key);
    }

    @Override
    public Map<Long, MetadataAwareValue<CustomKfkaMessage>> loadAll(Collection<Long> keys)
    {
        return keys.stream().collect(Collectors.toMap(k -> k, this::load));
    }

    @Override
    public Iterable<Long> loadAllKeys()
    {
        return map.keySet();
    }

    @Override
    public Duration getTTL()
    {
        return ttl;
    }
}
