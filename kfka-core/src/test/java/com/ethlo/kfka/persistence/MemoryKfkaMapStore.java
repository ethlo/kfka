package com.ethlo.kfka.persistence;

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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaMapStore;

public class MemoryKfkaMapStore<T> implements KfkaMapStore<CustomKfkaMessage>
{
    private ConcurrentMap<Long , CustomKfkaMessage> map = new ConcurrentHashMap<>();
    
    @Override
    public void store(Long key, CustomKfkaMessage value)
    {
        map.put(key, value);
    }

    @Override
    public void storeAll(Map<Long, CustomKfkaMessage> map)
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
        keys.forEach(key->map.remove(key));
    }

    @Override
    public CustomKfkaMessage load(Long key)
    {
        return map.get(key);
    }

    @Override
    public Map<Long, CustomKfkaMessage> loadAll(Collection<Long> keys)
    {
        //return keys.stream().collect(Collectors.toMap(k->k, k->map.get(k)));
        final Map<Long, CustomKfkaMessage> retVal = new HashMap<>(keys.size());
        keys.forEach(k->{
            final CustomKfkaMessage val = map.get(k);
            if (val != null)
            {
                retVal.put(k, val);
            }
        });
        return retVal;
    }

    @Override
    public Iterable<Long> loadAllKeys()
    {
        return map.keySet();
    }
}
