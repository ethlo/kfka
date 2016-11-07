package com.acme;

import java.util.Collections;

import com.ethlo.kfka.KfkaMessage;

public class CustomKfkaMessage extends KfkaMessage
{
    private static final long serialVersionUID = 443035395722534117L;
    private Integer userId;

    public CustomKfkaMessage(KfkaMessage message)
    {
        // TODO: Check we have all props
        super(new KfkaMessage.Builder()
            .payload(message.getPayload())
            .timestamp(message.getTimestamp())
            .topic(message.getTopic())
            .type(message.getType()));
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
}