package com.acme;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import com.ethlo.kfka.KfkaMessage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class CustomKfkaMessage extends KfkaMessage
{
    private static final long serialVersionUID = 443035395722534117L;
    private Integer userId;

    private CustomKfkaMessage()
    {
        super(null);
    }
    
    public CustomKfkaMessage(CustomKfkaMessageBuilder builder)
    {
        super(builder);
        this.userId = builder.userId;
    }
    
    public static class CustomKfkaMessageBuilder extends KfkaMessage.Builder
    {
        private int userId;

        @SuppressWarnings("unchecked")
        @Override
        public KfkaMessage build()
        {
            return new CustomKfkaMessage(this);
        }

        public CustomKfkaMessageBuilder userId(int userId)
        {
            this.userId = userId;
            return this;
        }
    }
    
    public CustomKfkaMessage setUserId(Integer userId)
    {
        this.userId = userId;
        return this;
    }

    public Integer getUserId()
    {
        return userId;
    }

    @Override
    public Collection<String> getQueryableProperties()
    {
        return Arrays.asList("userId");
    }

    @Override
    protected void doWriteData(ObjectDataOutput out) throws IOException
    {
        out.writeInt(userId != null ? userId : 0);
    }

    @Override
    public void doReadData(ObjectDataInput in) throws IOException
    {
        final int i = in.readInt();
        this.userId = i > 0 ? i : null;  
    }   
}