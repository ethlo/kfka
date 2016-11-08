package com.ethlo.kfka;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.util.Assert;

import com.google.common.base.Throwables;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

@SuppressWarnings("rawtypes")
public abstract class KfkaMessage implements Serializable, Comparable<KfkaMessage>, DataSerializable
{
    private static final long serialVersionUID = 3209315651061823360L;

    private String topic;
    private long timestamp;
    private byte[] payload;
    private String type;
    private Long id;

    public String getTopic()
    {
        return topic;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public byte[] getPayload()
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
        private byte[] payload;
        private String type;
        private Long id;

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
            this.payload = message.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        public Builder payload(byte[] payload)
        {
            this.payload = payload;
            return this;
        }

        public Builder type(String type)
        {
            this.type = type;
            return this;
        }

        public Builder id(Long id)
        {
            this.id = id;
            return this;
        }
    }

    protected KfkaMessage(Builder builder)
    {
        if (builder == null)
        {
            return;
        }
        
        Assert.notNull(builder.topic);
        Assert.notNull(builder.type);
        Assert.notNull(builder.timestamp);
        Assert.notNull(builder.payload);

        this.topic = builder.topic;
        this.timestamp = builder.timestamp;
        this.payload = builder.payload;
        this.type = builder.type;
        this.id = builder.id;
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

    public abstract Collection<String> getQueryableProperties();

    public static Object getPropertyValue(Object object, String propertyName)
    {
        Field field;
        try
        {
            field = object.getClass().getDeclaredField(propertyName);
            field.setAccessible(true);
            return field.get(object);
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException exc)
        {
            return null;
        }
    }

    public static Map<String, Comparable> getPropertyValues(Object object)
    {
        try
        {
            final Field[] fields = object.getClass().getDeclaredFields();
            final Map<String, Comparable> queryableFields = new TreeMap<>();
            for (Field field : fields)
            {
                if (Comparable.class.isAssignableFrom(field.getType()))
                {
                    field.setAccessible(true);
                    final Object o = field.get(object);
                    queryableFields.put(field.getName(), Comparable.class.cast(o));
                }
            }
            return queryableFields;
        }
        catch (SecurityException | IllegalArgumentException | IllegalAccessException exc)
        {
            throw Throwables.propagate(exc);
        } 
    }
    
    @Override
    public void writeData(ObjectDataOutput out) throws IOException
    {
        out.writeLong(id);
        out.writeUTF(topic);
        out.writeUTF(type);
        out.writeLong(timestamp);
        out.writeByteArray(payload);

        doWriteData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException
    {
        id = in.readLong();
        topic = in.readUTF();
        type = in.readUTF();
        timestamp = in.readLong();
        payload = in.readByteArray();
        
        doReadData(in);  
    }  

    protected abstract void doWriteData(ObjectDataOutput out) throws IOException;

    protected abstract void doReadData(ObjectDataInput in) throws IOException;
}
