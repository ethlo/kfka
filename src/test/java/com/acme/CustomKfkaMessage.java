package com.acme;

import java.util.Arrays;
import java.util.Collection;

import com.ethlo.kfka.KfkaMessage;

public class CustomKfkaMessage extends KfkaMessage
{
    private static final long serialVersionUID = 443035395722534117L;
    private Integer userId;

    public CustomKfkaMessage(KfkaMessage.Builder builder)
    {
        super(builder);
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
}