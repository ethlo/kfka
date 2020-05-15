package com.ethlo.kfka.persistence;

/*-
 * #%L
 * kfka
 * %%
 * Copyright (C) 2017 Morten Haraldsen (ethlo)
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

import com.ethlo.kfka.KfkaMessage;
import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.EntryStore;

public interface KfkaMapStore<T extends KfkaMessage> extends EntryLoader<Long, T>, EntryStore<Long, T>
{
    static long entryTTL(KfkaMessage value, Duration ttl)
    {
        final long now = System.currentTimeMillis();
        return Math.max(0, ((value.getTimestamp() + ttl.toMillis()) - now));
    }

    default MetadataAwareValue<T> wrap(T payload)
    {
        return new MetadataAwareValue<>(payload, getTTL().toMillis());
    }

    Duration getTTL();
}
