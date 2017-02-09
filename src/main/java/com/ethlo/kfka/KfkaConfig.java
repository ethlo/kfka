package com.ethlo.kfka;

import java.util.concurrent.TimeUnit;

public class KfkaConfig
{
    private String name = "kfka";
    private long ttlMillis = 0; // Forever
    private int writeDelay = 0; // Direct
    private int batchSize = 500;

    /**
     * Time to live for the event. Use 0 for forever.
     * @param duration
     * @param unit
     * @return
     */
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

    /**
     * The number of seconds to wait before writing to storage. Set to 0 for direct writing (write-through), or any positive integer for batch writing (write-behind). 
     * Batching will usually improve performance significantly at the risk of loosing data if all nodes in the cluster is going down.
     * @param seconds
     * @return
     */
    public KfkaConfig writeDelay(int seconds)
    {
        this.writeDelay = seconds;
        return this;
    }
    
    public int getWriteDelay()
    {
        return writeDelay;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public KfkaConfig batchSize(int size)
    {
        this.batchSize = size;
        return this;
    }
}
