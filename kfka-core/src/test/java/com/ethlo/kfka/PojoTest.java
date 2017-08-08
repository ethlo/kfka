package com.ethlo.kfka;

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

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

import com.acme.CustomKfkaMessage;


public class PojoTest
{
    @Test
    public void testHashcode()
    {
        new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build().hashCode();
    }
    
    @Test
    public void testEquals()
    {
        final CustomKfkaMessage a = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build();
        final CustomKfkaMessage b = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(12131L).topic("bar").type("foo").payload("payload").build();
        final CustomKfkaMessage c = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build();
                        
        assertThat(a).isNotEqualTo(b);
        assertThat(a).isEqualTo(c);
    }
    
    @Test
    public void testToString()
    {
        new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build().toString();
    }
}