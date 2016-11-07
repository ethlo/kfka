package com.ethlo.kfka;

import java.io.Serializable;

import org.springframework.util.Assert;

public class KfkaMessage implements Serializable, Comparable<KfkaMessage>
{
    private static final long serialVersionUID = 3209315651061823360L;
    
    private String topic;
    private long timestamp;
    private String message;
    private String type;
    private long userId;
    private long organzationId;

    private long id;

    public String getTopic()
    {
        return topic;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getMessage()
    {
        return message;
    }

    public String getType()
    {
        return type;
    }

    public long getUserId()
    {
        return userId;
    }

    public long getOrganzationId()
    {
        return organzationId;
    }

    public static class Builder
    {
        private String topic;
        private long timestamp;
        private String message;
        private String type;
        private long userId;
        private long organzationId;

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

        public Builder message(String message)
        {
            this.message = message;
            return this;
        }

        public Builder type(String type)
        {
            this.type = type;
            return this;
        }

        public Builder userId(long userId)
        {
            this.userId = userId;
            return this;
        }

        public Builder organzationId(long organzationId)
        {
            this.organzationId = organzationId;
            return this;
        }

        public KfkaMessage build()
        {
            return new KfkaMessage(this);
        }
    }

    private KfkaMessage(Builder builder)
    {
        Assert.notNull(builder.topic);
        Assert.notNull(builder.type);
        Assert.notNull(builder.timestamp);
        Assert.notNull(builder.message);
        
        this.topic = builder.topic;
        this.timestamp = builder.timestamp;
        this.message = builder.message;
        this.type = builder.type;
        this.userId = builder.userId;
        this.organzationId = builder.organzationId;
    }

    @Override
    public int compareTo(KfkaMessage o)
    {
        return Long.compare(o.id, this.id);
    }

    @Override
    public String toString()
    {
        return "KfkaMessage [id=" + id + ", " + (topic != null ? "topic=" + topic + ", " : "") + "timestamp=" + timestamp + ", " + (message != null ? "message=" + message + ", " : "")
                        + (type != null ? "type=" + type + ", " : "") + "userId=" + userId + ", organzationId=" + organzationId + "]";
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
}
