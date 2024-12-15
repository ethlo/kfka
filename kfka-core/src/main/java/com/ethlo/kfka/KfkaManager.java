package com.ethlo.kfka;

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

import java.util.List;

public interface KfkaManager<T extends KfkaMessage>
{
    void addAll(List<T> msgs);

    long size();

    default void addListener(KfkaMessageListener<T> listener)
    {
        addListener(listener, new KfkaPredicate());
    }

    void addListener(KfkaMessageListener<T> listener, KfkaPredicate kfkaPredicate);

    void clear();

    int addListener(KfkaMessageListener<T> listener, KfkaPredicate kfkaPredicate, String lastMessageId);

    int addListener(KfkaMessageListener<T> listener, KfkaPredicate kfkaPredicate, int rewind);

    void removeListener(KfkaMessageListener<T> listener);

    void evictExpired();

    default T add(T value)
    {
        addAll(List.of(value));
        return value;
    }

    default void addListener(KfkaMessageListener<T> listener, String lastSeenMessageId)
    {
        addListener(listener, new KfkaPredicate(), lastSeenMessageId);
    }
}
