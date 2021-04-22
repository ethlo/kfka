package com.ethlo.kfka;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2021 Morten Haraldsen (ethlo)
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

public class KfkaConfig
{
    private String name = "kfka";
    private Duration ttl = Duration.ZERO; // Forever
    private int batchSize = 500;
    private Duration cleanInterval = Duration.ofHours(1);

    /**
     * Time to live for the event. Use 0 for forever.
     *
     * @param ttl The duration the entity will be kept
     * @return This configuration (for fluent programming)
     */
    public KfkaConfig ttl(Duration ttl)
    {
        this.ttl = ttl;
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

    public int getBatchSize()
    {
        return batchSize;
    }

    public KfkaConfig batchSize(int size)
    {
        this.batchSize = size;
        return this;
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
