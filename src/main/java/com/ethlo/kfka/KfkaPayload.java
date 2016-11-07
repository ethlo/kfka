package com.ethlo.kfka;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class KfkaPayload implements Serializable
{
    private static final long serialVersionUID = 5418462384097244196L;
    
    private final Map<String, Comparable> queryableProperties;
    private final byte[] data;
    
    public KfkaPayload(String message)
    {
        this.data = message.getBytes(StandardCharsets.UTF_8);
        this.queryableProperties = Collections.emptyMap();
    }

    public KfkaPayload(String message, Map<String, Comparable> queryableProperties)
    {
        this.data = message.getBytes(StandardCharsets.UTF_8);
        this.queryableProperties = queryableProperties;
    }

    public KfkaPayload(byte[] payload, Map<String, Comparable> queryableProperties)
    {
        this.data = payload;
        this.queryableProperties = queryableProperties;
    }

    public Map<String, Comparable> getQueryableProperties()
    {
        return queryableProperties;
    }

    public byte[] getData()
    {
        return data;
    }
}
