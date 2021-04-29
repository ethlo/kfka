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

import java.util.Optional;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.KfkaPredicate;

public interface KfkaMessageStore
{
    <T extends KfkaMessage> T add(T message);

    long size();

    void clear();

    void sendAfter(String messageId, final KfkaPredicate predicate, KfkaMessageListener l);

    Optional<String> getMessageIdForRewind(final KfkaPredicate predicate);

    void clearExpired();

    void sendAll(KfkaPredicate predicate, KfkaMessageListener l);

    void sendIncluding(String messageId, KfkaPredicate predicate, KfkaMessageListener l);
}
