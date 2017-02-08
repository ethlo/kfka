package com.ethlo.kfka;

import java.util.concurrent.TimeUnit;

public class KfkaConfig
{
    private String name = "kfka";
    private long ttlMillis = 0;
    private int writeDelay = 0;

    public KfkaConfig ttl(long duration, TimeUnit unit)
    {
        this.ttlMillis = unit.toMillis(duration);
        return this;
    }

    public long getTtl(TimeUnit unit)
    {
        return unit.convert(ttlMillis, TimeUnit.MILLISECONDS);
    }
    
    public KfkaConfig name(String name)
    {
        this.name = name;
        return this;
    }

    public String getName()
    {
        return name;
    }

    public KfkaConfig writeDelay(int seconds)
    {
        this.writeDelay = seconds;
        return this;
    }
    
    public int getWriteDelay()
    {
        return writeDelay;
    }
}
