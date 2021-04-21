package com.ethlo.kfka;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2021 Morten Haraldsen (ethlo)
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaMessageStore;

public abstract class MapStoreTest
{
    @Autowired
    private KfkaMessageStore mapStore;

    @Test
    public void testStoreAll()
    {
        mapStore.add(new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(123L).build());
        mapStore.add(new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(129L).build());
        //final List<CustomKfkaMessage> output = mapStore.loadAll();
        //assertThat(output).isEqualTo(input);
    }

    @Test
    public void testStoreSingle()
    {
        final CustomKfkaMessage message = new CustomKfkaMessage.CustomKfkaMessageBuilder()
                .topic("foobar")
                .type("mytype")
                .payload("hello")
                .id(130L).build();
        mapStore.add(message);
        assertThat(mapStore.head().next()).isEqualTo(message);
    }
}
