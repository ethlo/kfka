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

import static com.ethlo.kfka.KfkaMessage.MESSAGE_ID_LENGTH;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ethlo.kfka.persistence.KfkaMessageStore;
import com.ethlo.kfka.util.RandomUtil;

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
    public KfkaMessage add(KfkaMessage msg)
    {
        if (msg.getTimestamp() == null)
        {
            msg.timestamp(System.currentTimeMillis());
        }

        if (msg.getMessageId() == null)
        {
            msg.setMessageId(RandomUtil.generateAsciiString(MESSAGE_ID_LENGTH));
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

        return msg;
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
        if (kfkaPredicate.getMessageId() != null)
        {
            // We have a message id
            kfkaMessageStore.sendAfter(kfkaPredicate.getMessageId(), kfkaPredicate, l);
        }
        else if (kfkaPredicate.getRewind() != null)
        {
            // We have rewind
            sendDataWithRewind(kfkaPredicate, l);
        }

        // Add to set of listeners, with the desired predicate
        msgListeners.put(l, kfkaPredicate);

        return l;
    }

    private void sendDataWithRewind(final KfkaPredicate predicate, KfkaMessageListener l)
    {
        final Optional<String> messageId = kfkaMessageStore.getMessageIdForRewind(predicate);
        if (messageId.isPresent())
        {
            // We found a message to start from
            messageId.ifPresent(id -> kfkaMessageStore.sendIncluding(id, predicate, l));
        }
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
