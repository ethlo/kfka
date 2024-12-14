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

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.kfka.persistence.KfkaMessageStore;

public class KfkaManagerImpl<T extends KfkaMessage> implements KfkaManager<T>
{
    private static final Logger logger = LoggerFactory.getLogger(KfkaManagerImpl.class);
    private final KfkaMessageStore<T> kfkaMessageStore;
    private final ConcurrentMap<KfkaMessageListener<T>, KfkaPredicate> msgListeners = new ConcurrentHashMap<>();
    private final MessageIdGenerator messageIdGenerator;

    public KfkaManagerImpl(KfkaMessageStore<T> kfkaMessageStore, MessageIdGenerator messageIdGenerator)
    {
        this.kfkaMessageStore = kfkaMessageStore;
        this.messageIdGenerator = messageIdGenerator;
    }

    @Override
    public void addListener(KfkaMessageListener<T> l)
    {
        this.addListener(l, new KfkaPredicate());
    }

    @Override
    public void addAll(List<T> messages)
    {
        final OffsetDateTime now = OffsetDateTime.now();
        messages.forEach(msg ->
        {
            if (msg.getTimestamp() == null)
            {
                msg.timestamp(now);
            }

            if (msg.getMessageId() == null)
            {
                msg.setMessageId(messageIdGenerator.get());
                logger.trace("Adding {}", msg.getMessageId());
            }
        });

        kfkaMessageStore.addAll(messages);

        // Push real-time
        for (final Map.Entry<KfkaMessageListener<T>, KfkaPredicate> e : msgListeners.entrySet())
        {
            messages.forEach(msg ->
            {
                final KfkaMessageListener<T> l = e.getKey();
                final KfkaPredicate p = e.getValue();
                if (p.matches(msg))
                {
                    l.onMessage(msg);
                }
            });
        }
    }

    @Override
    public long size()
    {
        return kfkaMessageStore.size();
    }

    @Override
    public void clear()
    {
        this.kfkaMessageStore.clear();
    }

    public KfkaMessageListener<T> addListener(KfkaMessageListener<T> listener, KfkaPredicate kfkaPredicate)
    {
        if (kfkaPredicate.getMessageId() != null)
        {
            // We have a message id
            kfkaMessageStore.sendAfter(kfkaPredicate.getMessageId(), kfkaPredicate, listener);
        }
        else if (kfkaPredicate.getRewind() != null)
        {
            // We have rewind
            sendDataWithRewind(kfkaPredicate, listener);
        }

        // Add to set of listeners, with the desired predicate
        msgListeners.put(listener, kfkaPredicate);

        return listener;
    }

    private void sendDataWithRewind(final KfkaPredicate predicate, KfkaMessageListener<T> listener)
    {
        final Optional<String> messageId = kfkaMessageStore.getMessageIdForRewind(predicate);
        if (messageId.isPresent())
        {
            // We found a message to start from
            kfkaMessageStore.sendIncluding(messageId.get(), predicate, listener);
        }
        else
        {
            // What should we do here
        }
    }

    @Override
    public void removeListener(KfkaMessageListener<T> listener)
    {
        this.msgListeners.remove(listener);
    }

    @Override
    public void evictExpired()
    {
        this.kfkaMessageStore.clearExpired();
    }
}
