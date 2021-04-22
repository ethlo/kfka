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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ethlo.kfka.persistence.KfkaMessageStore;

public class KfkaManagerImpl implements KfkaManager
{
    private final KfkaMessageStore kfkaMessageStore;
    private final ConcurrentMap<KfkaMessageListener, KfkaPredicate> msgListeners = new ConcurrentHashMap<>();

    public KfkaManagerImpl(KfkaMessageStore kfkaMessageStore)
    {
        this.kfkaMessageStore = kfkaMessageStore;
    }

    @Override
    public void addListener(KfkaMessageListener l)
    {
        this.addListener(l, new KfkaPredicate());
    }

    @Override
    public long add(KfkaMessage msg)
    {
        if (msg.getTimestamp() == null)
        {
            msg.timestamp(System.currentTimeMillis());
        }

        kfkaMessageStore.add(msg);

        // Push real-time
        for (final Map.Entry<KfkaMessageListener, KfkaPredicate> e : msgListeners.entrySet())
        {
            final KfkaMessageListener l = e.getKey();
            final KfkaPredicate p = e.getValue();
            if (p.matches(msg))
            {
                l.onMessage(msg);
            }
        }

        return msg.getId();
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

    public KfkaMessageListener addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate)
    {
        // Relative offset
        final Integer offset = kfkaPredicate.getRelativeOffset();
        if (offset == null && kfkaPredicate.getMessageId() != null)
        {
            // We have just a message id
            sendAfter(kfkaPredicate.getMessageId(), kfkaPredicate, l);
        }
        else if (offset != null)
        {
            sendDataWithOffset(kfkaPredicate, l);
        }

        // Add to set of listeners, with the desired predicate
        msgListeners.put(l, kfkaPredicate);

        return l;
    }

    private void sendDataWithOffset(final KfkaPredicate predicate, KfkaMessageListener l)
    {
        final Optional<Long> messageId = kfkaMessageStore.getOffsetMessageId(predicate.getRelativeOffset(), predicate);
        if (messageId.isPresent())
        {
            messageId.ifPresent(id -> sendAfter(id, predicate, l));
        }
        else
        {
            // All as we did not find a message that far back
            sendAfter(0, predicate, l);
        }
    }

    private void sendAfter(final long id, final KfkaPredicate predicate, KfkaMessageListener l)
    {
        kfkaMessageStore.sendAfter(id, predicate, l);
    }

    @Override
    public void removeListener(KfkaMessageListener listener)
    {
        this.msgListeners.remove(listener);
    }

    @Override
    public void evictExpired()
    {
        this.kfkaMessageStore.clearExpired();
    }
}
