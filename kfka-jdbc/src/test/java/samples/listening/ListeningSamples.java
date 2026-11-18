package samples.listening;

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

import static org.assertj.core.api.Assertions.assertThat;
import static samples.SampleSupport.kfkaManager;
import static samples.SampleSupport.send;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaPredicate;

class ListeningSamples
{
    /**
     * Listen for messages on a topic. The listener first gets (up to) the last 100 stored messages that match, then
     * every new one. The return value is the number of stored messages it was sent.
     */
    @Test
    void rewind()
    {
        final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();
        send(kfkaManager, "my_topic", 1);
        send(kfkaManager, "other_topic", 1);
        send(kfkaManager, "my_topic", 2);

        final List<CustomKfkaMessage> received = new ArrayList<>();
        final int replayed = kfkaManager.addListener(received::add, new KfkaPredicate().topic("my_topic"), 100);
        assertThat(replayed).isEqualTo(2);

        send(kfkaManager, "my_topic", 3);
        assertThat(received).hasSize(3);
    }

    /**
     * Use as a back-end for Server-Sent Events or WebSockets. A client that reconnects gets everything after the last
     * message id it saw, here only the messages for one user, filtered on a custom property.
     */
    @Test
    void resumeAfterLastSeenMessage()
    {
        final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();
        final CustomKfkaMessage seen = send(kfkaManager, "chat", 123);
        send(kfkaManager, "chat", 456);
        final CustomKfkaMessage missed = send(kfkaManager, "chat", 123);

        final String lastSeenMessageId = seen.getMessageId(); // For SSE, the Last-Event-ID header
        final List<CustomKfkaMessage> received = new ArrayList<>();
        final KfkaPredicate predicate = new KfkaPredicate().topic("chat").addPropertyMatch("userId", 123);
        kfkaManager.addListener(received::add, predicate, lastSeenMessageId);

        assertThat(received).containsExactly(missed);
    }
}
