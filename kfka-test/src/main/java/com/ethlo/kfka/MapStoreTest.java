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
        input.put(123L, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(123L).build());
        input.put(129L, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(129L).build());
        mapStore.storeAll(input);
        
        final Map<Long, CustomKfkaMessage> output = mapStore.loadAll(input.keySet());
        assertThat(output).isEqualTo(input);
    }
}
