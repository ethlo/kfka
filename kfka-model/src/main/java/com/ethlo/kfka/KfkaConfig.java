package com.ethlo.kfka;

/*-
 * #%L
 * kfka-model
 * %%
 * Copyright (C) 2017 - 2018 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.time.Duration;

import com.hazelcast.config.MapStoreConfig.InitialLoadMode;

public class KfkaConfig
{
    private String name = "kfka";
    private Duration ttl = Duration.ofMillis(0); // Forever
    private int writeDelay = 0; // Direct
    private int batchSize = 500;
    private InitialLoadMode initialLoadMode = InitialLoadMode.EAGER;
    private boolean persistent = true;
    private int maxQuerySize = Integer.MAX_VALUE;
    private boolean clearExpiredOnStartup = true;
    private Duration cleanInterval = Duration.ofHours(1);

    /**
     * Time to live for the event. Use 0 for forever.
     * @param ttl The duration the entity will be kept
     * @return This configuration (for fluent programming)
     */
    public KfkaConfig ttl(Duration ttl)
    {
        this.ttl = ttl;
        return this;
    }
    
    public KfkaConfig initialLoadMode(InitialLoadMode mode)
    {
        this.initialLoadMode = mode;
        return this;
    }
    
    /**
     * Whether to persist the messages to map store
     * @param persistent true if persistent, otherwise false
     * @return This configuration (for fluent programming)
     */
    public KfkaConfig persistent(boolean persistent)
    {
        this.persistent = persistent;
        return this;
    }

    public Duration getTtl()
    {
        return ttl;
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
     * @param seconds Write delay in seconds
     * @return This configuration (for fluent programming)
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

    public InitialLoadMode getInitialLoadMode()
    {
        return this.initialLoadMode;
    }

    public boolean isPersistent()
    {
        return persistent;
    }

    public int getMaxQuerySize()
    {
        return this.maxQuerySize ;
    }

    public boolean clearExpiredOnStartup()
    {
        return clearExpiredOnStartup ;
    }

    public Duration getCleanInterval()
    {
        return cleanInterval;
    }

    public void setCleanInterval(Duration cleanInterval)
    {
        this.cleanInterval = cleanInterval;
    }
}
