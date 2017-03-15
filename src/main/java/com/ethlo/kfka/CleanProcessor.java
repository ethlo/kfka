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

import java.util.Map.Entry;

import com.hazelcast.map.AbstractEntryProcessor;

public class CleanProcessor extends AbstractEntryProcessor<Long, KfkaMessage>
{
    private static final long serialVersionUID = 6953483674346132344L;

    @Override
    public Object process(Entry<Long, KfkaMessage> entry)
    {
        entry.setValue(null);
        return null;
    }        
}
