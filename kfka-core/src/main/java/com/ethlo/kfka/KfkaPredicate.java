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
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.ethlo.kfka.util.ReflectionUtil;

public class KfkaPredicate implements Serializable
{
    private final Map<String, Serializable> propertyMatch = new TreeMap<>();
    private String topic;
    private String type;

    public KfkaPredicate topic(String topic)
    {
        this.topic = topic;
        return this;
    }

    public KfkaPredicate type(String type)
    {
        this.type = type;
        return this;
    }

    public String getTopic()
    {
        return this.topic;
    }

    public String getType()
    {
        return this.type;
    }

    public KfkaPredicate addPropertyMatch(String propertyName, Serializable propertyValue)
    {
        this.propertyMatch.put(propertyName, propertyValue);
        return this;
    }

    public boolean matches(KfkaMessage msg)
    {
        final boolean basicMatch = (getType() == null || Objects.equals(msg.getType(), getType()))
                && (getTopic() == null || Objects.equals(msg.getTopic(), getTopic()));

        if (propertyMatch.isEmpty())
        {
            return basicMatch;
        }

        final Collection<Field> fields = ReflectionUtil.getFields(msg.getClass()).values();
        for (Map.Entry<String, Serializable> e : propertyMatch.entrySet())
        {
            final String propertyName = e.getKey();
            final Serializable filterValue = e.getValue();

            for (Field field : fields)
            {
                if (field.getName().equals(propertyName))
                {
                    try
                    {
                        final Object value = field.get(msg);
                        if (!Objects.equals(value, filterValue))
                        {
                            return false;
                        }
                    }
                    catch (IllegalAccessException exc)
                    {
                        throw new RuntimeException(exc);
                    }
                }
            }
        }
        return true;

    }

    public Map<String, Serializable> getPropertyMatch()
    {
        return propertyMatch;
    }
}
