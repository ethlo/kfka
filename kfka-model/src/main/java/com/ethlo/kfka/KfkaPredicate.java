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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

@SuppressWarnings("rawtypes")
public class KfkaPredicate implements Serializable
{
    private static final long serialVersionUID = -8869419733948277543L;

    private Integer relativeOffset;
    
    private Long messageId;
    
    // Filtering
    private String topic;
    private String type;
    
    // Support custom properties
    private Map<String, Comparable> propertyMatch = new TreeMap<>();
    
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

    public Long getMessageId()
    {
        return this.messageId;
    }
    
    public Integer getRelativeOffset()
    {
        return this.relativeOffset;
    }

    public KfkaPredicate relativeOffset(Integer relativeOffset)
    {
        this.relativeOffset = relativeOffset;
        return this;
    }
    
    public KfkaPredicate messageId(long offsetId)
    {
        this.messageId = offsetId;
        return this;
    }

    public java.util.function.Predicate<KfkaMessage> toPredicate()
    {
        return new java.util.function.Predicate<KfkaMessage>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public boolean test(KfkaMessage input)
            {
                if (topic != null && !topic.equalsIgnoreCase(input.getTopic()))
                {
                    return false;
                }
                
                final Map<String, Comparable> queryableProperties = KfkaMessage.getPropertyValues(input);
                if (! propertyMatch.isEmpty() && !queryableProperties.isEmpty())
                {
                    for (Entry<String, Comparable> e : propertyMatch.entrySet())
                    {
                        final String propertyName = e.getKey();
                        final Comparable propertyValue = e.getValue();
                        final Comparable toMatch = queryableProperties.get(propertyName);
                        if (toMatch != null && toMatch.compareTo(propertyValue) != 0)
                        {
                            return false;
                        }
                    }
                }
                
                return true;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public Predicate<Long, KfkaMessage> toHazelcastPredicate()
    {
        final List<Predicate<?,?>> predicates = new LinkedList<>();
        
        // Message ID
        if (messageId != null)
        {
            predicates.add(Predicates.greaterEqual("id", messageId));
        }
        
        // Topic
        if (topic != null)
        {
            predicates.add(Predicates.equal("topic", topic));
        }
        
        if (! propertyMatch.isEmpty())
        {
            for (Entry<String, Comparable> e : propertyMatch.entrySet())
            {
                predicates.add(Predicates.equal(e.getKey(), e.getValue()));
            }
        }
        
        return Predicates.and(predicates.toArray(new Predicate[predicates.size()]));
    }

    public KfkaPredicate addPropertyMatch(String propertyName, Comparable propertyValue)
    {
        this.propertyMatch.put(propertyName, propertyValue);
        return this;
    }
    
    public KfkaPredicate setPropertyMatch(Map<String, Comparable> propertyMatch)
    {
        Assert.notNull(propertyMatch, "propertyMatch cannot be null");
        this.propertyMatch = propertyMatch;
        return this;
    }
}
