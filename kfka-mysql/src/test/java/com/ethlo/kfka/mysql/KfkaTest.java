package com.ethlo.kfka.mysql;

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

import java.time.OffsetDateTime;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.CollectingListener;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaPredicate;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KfkaTest
{
    private final Logger logger = LoggerFactory.getLogger(KfkaTest.class);

    @Autowired
    private KfkaManager<CustomKfkaMessage> kfkaManager;

    @Test
    public void testQueryLast()
    {
        kfkaManager.clear();

        final List<String> messageIds = new LinkedList<>();
        for (int i = 0; i < 1_000; i++)
        {
            final CustomKfkaMessage msg = kfkaManager.add(new CustomKfkaMessageBuilder().payload("" + i).topic("mytopic").type("mytype").build());
            messageIds.add(msg.getMessageId());
        }

        final List<KfkaMessage> received = new LinkedList<>();
        kfkaManager.addListener(received::add, new KfkaPredicate().lastSeenMessageId(messageIds.get(messageIds.size() - 2)));
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage").topic("mytopic").type("mytype").build());

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getPayload()).isEqualTo("999" .getBytes());
        assertThat(received.get(1).getPayload()).isEqualTo("myMessage" .getBytes());
    }

    @Test
    public void testQuerySinceBeginningFilteredByTopic()
    {
        kfkaManager.clear();

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").build());

        final List<KfkaMessage> received = new LinkedList<>();
        final KfkaPredicate predicate = new KfkaPredicate().topic("bar").rewind(100);
        this.kfkaManager.addListener(received::add, predicate);

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getId()).isEqualTo(2);
        assertThat(received.get(1).getId()).isEqualTo(4);
    }

    @Test
    public void testOnlyNewMessages()
    {
        kfkaManager.clear();

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").build());

        final List<KfkaMessage> received = new LinkedList<>();
        final KfkaPredicate predicate = new KfkaPredicate().topic("bar");
        this.kfkaManager.addListener(received::add, predicate);

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage5").topic("bar").type("mytype").build());

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(5);
    }

    @Test
    public void testOnlyNewMessagesWhenManyOld()
    {
        kfkaManager.clear();

        for (int i = 0; i < 10_000; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().payload("oldMessage" + (i + 1)).topic("foo").type("mytype").build());
        }

        final KfkaPredicate predicate = new KfkaPredicate().topic("foo").rewind(10);
        final List<KfkaMessage> received = new LinkedList<>();
        this.kfkaManager.addListener(received::add, predicate);

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("Message").topic("foo").type("mytype").build());

        assertThat(received).hasSize(11);
        assertThat(received.get(0).getId()).isEqualTo(9_991);
    }

    @Test
    public void testCleanOldMessages()
    {
        final String type = "mytype";

        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type(type).build());

        final List<KfkaMessage> messages = new LinkedList<>();

        kfkaManager.addListener(messages::add, new KfkaPredicate().rewind(100));
        assertThat(messages).hasSize(4);

        messages.clear();
        kfkaManager.clear();

        kfkaManager.addListener(messages::add, new KfkaPredicate().rewind(100));
        assertThat(messages).isEmpty();
    }

    @Test
    public void testSize()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        assertThat(kfkaManager.size()).isEqualTo(2);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopic()
    {
        final String type = "my_type";

        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type(type).build());

        final List<KfkaMessage> received = new LinkedList<>();
        kfkaManager.addListener(received::add, new KfkaPredicate().topic("bar").rewind(1));

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(4);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopicAndCustomProperty() throws InterruptedException
    {
        kfkaManager.clear();

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(321).payload("myMessage1").topic("bar").type("mytype").build());

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage2").topic("bar").type("mytype").build());

        final CollectingListener<CustomKfkaMessage> collListener = new CollectingListener<>();
        kfkaManager.addListener(collListener, new KfkaPredicate().topic("bar").rewind(10).addPropertyMatch("userId", 123));

        assertThat(collListener.getReceived()).hasSize(1);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(2);

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage3").topic("bar").type("mytype").build());

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(321).payload("myMessage4").topic("bar").type("mytype").build());

        // Need to allow asynchronous messages propagate
        Thread.sleep(100);

        assertThat(collListener.getReceived()).hasSize(2);
        assertThat(collListener.getReceived().get(1).getId()).isEqualTo(3);
    }

    @Test
    public void testLastMessageId()
    {
        kfkaManager.clear();
        final CustomKfkaMessage msg1 = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").payload("msg1").type("mytype").build());

        final CustomKfkaMessage msg2 = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").payload("msg2").build());

        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.addListener(l, new KfkaPredicate().lastSeenMessageId(msg1.getMessageId()));
        assertThat(l.getReceived()).hasSize(1);
        assertThat(l.getReceived()).contains(msg2);
    }

    @Test
    public void testMessagesPersisted()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.addListener(l, new KfkaPredicate().rewind(1));
        assertThat(l.getReceived()).hasSize(1);
    }

    @Test
    public void testMessagesExpire()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").timestamp(OffsetDateTime.parse("2000-01-01T00:00:00Z")).type("mytype").build());
        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.evictExpired();
        kfkaManager.addListener(l, new KfkaPredicate().rewind(1_000));
        assertThat(l.getReceived()).isEmpty();
    }

    @Test
    public void testPerformance1()
    {
        kfkaManager.clear();

        final int count = 100_000;
        final StopWatch sw = new StopWatch();
        sw.start("Insert1");
        for (int i = 1; i <= count; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().userId(321).payload("otherMessage" + i).topic("bar").type("mytype").build());
        }
        sw.stop();

        sw.start("Insert2");
        for (int i = 1; i <= count; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage" + 1).topic("bar").type("mytype").build());
        }
        sw.stop();

        sw.start("Query");
        final CollectingListener<CustomKfkaMessage> collListener = new CollectingListener<>();
        kfkaManager.addListener(collListener, new KfkaPredicate().topic("bar").rewind(count + 10).addPropertyMatch("userId", 123));
        sw.stop();
        assertThat(collListener.getReceived()).hasSize(count);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(count + 1);
        assertThat(collListener.getReceived().get(count - 1).getId()).isEqualTo(count + count);
        logger.info("Timings: {}", sw);
    }
}
