package com.ethlo.kfka;

import java.io.Serializable;
import java.util.Map;

import org.springframework.util.Assert;

@SuppressWarnings("rawtypes")
public class KfkaMessage implements Serializable, Comparable<KfkaMessage>
{
    private static final long serialVersionUID = 3209315651061823360L;
    
    private String topic;
    private long timestamp;
    private KfkaPayload payload;
    private String type;

    private long id;

    public String getTopic()
    {
        return topic;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public KfkaPayload getPayload()
    {
        return payload;
    }

    public String getType()
    {
        return type;
    }
    
    public static class Builder
    {
        private String topic;
        private long timestamp;
        private KfkaPayload payload;
        private String type;

        public Builder topic(String topic)
        {
            this.topic = topic;
            return this;
        }

        public Builder timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public Builder payload(String message)
        {
            this.payload = new KfkaPayload(message);
            return this;
        }
        
        public Builder payload(String message, Map<String, Comparable> queryableProperties)
        {
            this.payload = new KfkaPayload(message, queryableProperties);
            return this;
        }
        
        public Builder payload(byte[] payload, Map<String, Comparable> queryableProperties)
        {
            this.payload = new KfkaPayload(payload, queryableProperties);
            return this;            
        }

        public Builder type(String type)
        {
            this.type = type;
            return this;
        }

        public KfkaMessage build()
        {
            return new KfkaMessage(this);
        }

        public Builder payload(KfkaPayload payload)
        {
            this.payload = payload;
            return this;
        }
    }

    protected KfkaMessage(Builder builder)
    {
        Assert.notNull(builder.topic);
        Assert.notNull(builder.type);
        Assert.notNull(builder.timestamp);
        Assert.notNull(builder.payload);
        
        this.topic = builder.topic;
        this.timestamp = builder.timestamp;
        this.payload = builder.payload;
        this.type = builder.type;
    }

    @Override
    public int compareTo(KfkaMessage o)
    {
        return Long.compare(o.id, this.id);
    }

    @Override
    public String toString()
    {
        return "KfkaMessage [id=" + id + ", " + (topic != null ? "topic=" + topic + ", " : "") + "timestamp=" + timestamp + ", " + (payload != null ? "payload=" + payload + ", " : "")
                        + (type != null ? "type=" + type + ", " : "");
    }

    KfkaMessage id(long id)
    {
        this.id = id;
        return this;
    }

    public Long getId()
    {
        return this.id;
    }

    public Map<String, Comparable> getQueryableProperties()
    {
        return this.payload.getQueryableProperties();
    }
}
