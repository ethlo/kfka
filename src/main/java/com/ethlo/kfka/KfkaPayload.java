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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class KfkaPayload implements Serializable
{
    private static final long serialVersionUID = 5418462384097244196L;
    
    private final Map<String, Comparable> queryableProperties;
    private final byte[] data;
    
    public KfkaPayload(String message)
    {
        this.data = message.getBytes(StandardCharsets.UTF_8);
        this.queryableProperties = Collections.emptyMap();
    }

    public KfkaPayload(String message, Map<String, Comparable> queryableProperties)
    {
        this.data = message.getBytes(StandardCharsets.UTF_8);
        this.queryableProperties = queryableProperties;
    }

    public KfkaPayload(byte[] payload, Map<String, Comparable> queryableProperties)
    {
        this.data = payload;
        this.queryableProperties = queryableProperties;
    }

    public Map<String, Comparable> getQueryableProperties()
    {
        return queryableProperties;
    }

    public byte[] getData()
    {
        return data;
    }
}
