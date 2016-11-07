package com.ethlo.kfka;

public class KfkaPredicate
{
    private final KfkaManager kfkaManager;
    private Integer offset = null;
    private Long offsetId;
    
    // Filtering
    private String topic;
    private String type;
    
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

    public void addListener(KfkaMessageListener kfkaMessageListener)
    {
        kfkaManager.addListener(kfkaMessageListener, this);
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

}
