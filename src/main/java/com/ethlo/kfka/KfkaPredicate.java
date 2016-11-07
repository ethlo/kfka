package com.ethlo.kfka;

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

    private final KfkaManager kfkaManager;
    private Integer offset = null;
    private Long offsetId;
    
    // Filtering
    private String topic;
    private String type;
    
    // Support custom properties
    private Map<String, Comparable> propertyMatch = new TreeMap<>();
    
    KfkaPredicate(KfkaManager kfkaManager)
    {
        this.kfkaManager = kfkaManager;
    }

    public KfkaPredicate seekToBeginning()
    {
        final long lowestMatch = kfkaManager.findfirst(topic, type);
        if (lowestMatch != Long.MAX_VALUE)
        {
            offsetId = lowestMatch;
            offset = -Integer.MAX_VALUE;
        }
        else
        {
            offsetId = null;
        }
        
        return this;
    }
    
    public void seek(long offsetId)
    {
        this.offsetId = offsetId;
    }
    
    public void seekToEnd()
    {
        this.offsetId = kfkaManager.findLatest(topic, type);
    }

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

    public void addListener(KfkaMessageListener l)
    {
        kfkaManager.addListener(l, this);
    }

    public String getTopic()
    {
        return this.topic;
    }
    
    public String getType()
    {
        return this.type;
    }

    public Long getOffsetId()
    {
        return this.offsetId;
    }
    
    public Integer getOffset()
    {
        return this.offset;
    }

    public KfkaPredicate offset(Integer offset)
    {
        this.offset = offset;
        return this;
    }
    
    public KfkaPredicate offsetId(long offsetId)
    {
        this.offsetId = offsetId;
        return this;
    }

    public com.google.common.base.Predicate<KfkaMessage> toGuavaPredicate()
    {
        return new com.google.common.base.Predicate<KfkaMessage>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public boolean apply(KfkaMessage input)
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

    public Predicate<?,?> toHazelcastPredicate()
    {
        final List<Predicate<?,?>> predicates = new LinkedList<>();
        
        // Position
        if (offsetId != null)
        {
            predicates.add(Predicates.greaterEqual("id", offsetId));
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

    public KfkaPredicate propertyMatch(Map<String, Comparable> propertyMatch)
    {
        this.propertyMatch = propertyMatch;
        return this;
    }

    public static KfkaPredicate rewind(KfkaManager manager, int count)
    {
        return new KfkaPredicate(manager).offset(-count);
    }
}
