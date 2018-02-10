package com.ethlo.kfka;

/*-
 * #%L
 * kfka-test
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

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaMapStore;

public abstract class MapStoreTest
{
    @Autowired
    private KfkaMapStore<CustomKfkaMessage> mapStore;
    
    @Test
    public void testStoreAll()
    {
        final Map<Long, CustomKfkaMessage> input = new TreeMap<>();
        final long key1 = 123;
        final long key2 = 129;
        input.put(key1, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(key1).build());
        input.put(key2, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(key2).build());
        mapStore.storeAll(input);
        
        final Map<Long, CustomKfkaMessage> output = mapStore.loadAll(input.keySet());
        assertThat(output).isEqualTo(input);
    }
    
    @Test
    public void testStoreSingle()
    {
        final long id = 130L; 
        final CustomKfkaMessage message = new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(id).build();
        mapStore.store(id, message);
        
        final CustomKfkaMessage output = mapStore.load(id);
        assertThat(output).isEqualTo(message);
    }
    
    @Test
    public void loadAllKeys()
    {
        final long key1 = 135;
        final long key2 = 136;
        final Map<Long, CustomKfkaMessage> input = new TreeMap<>();
        input.put(key1, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(key1).build());
        input.put(key2, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(key2).build());
        mapStore.storeAll(input);
        
        final Iterable<Long> allKeys = mapStore.loadAllKeys();
        assertThat(allKeys).contains(135L, 136L);
    }
    
    @Test
    public void testDeleteAll()
    {
        final Map<Long, CustomKfkaMessage> input = new TreeMap<>();
        final long key1 = 140;
        final long key2 = 141;
        final Collection<Long> keys = Arrays.asList(key1, key2);
        
        input.put(key1, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(key1).build());
        input.put(key2, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(key2).build());
        mapStore.storeAll(input);
        assertThat(mapStore.load(key1)).isNotNull();
        assertThat(mapStore.load(key2)).isNotNull();
        
        mapStore.deleteAll(keys);
        
        final Map<Long, CustomKfkaMessage> output = mapStore.loadAll(keys);
        assertThat(output).isEmpty();
    }
    
    @Test
    public void testClearExpired()
    {
        final Map<Long, CustomKfkaMessage> input = new TreeMap<>();
        final long key1 = 150;
        final long key2 = 151;
        input.put(key1, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(key1).build());
        input.put(key2, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(key2).build());
        mapStore.storeAll(input);
        mapStore.clearExpired();
    }
}
