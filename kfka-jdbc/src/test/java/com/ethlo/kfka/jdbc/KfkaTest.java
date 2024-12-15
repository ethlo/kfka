package com.ethlo.kfka.jdbc;

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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StopWatch;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.CollectingListener;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaPredicate;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestCfg.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KfkaTest
{
    private final Logger logger = LoggerFactory.getLogger(KfkaTest.class);

    @Autowired
    private KfkaManager<CustomKfkaMessage> kfkaManager;

    @Test
    void testQueryLast()
    {
        kfkaManager.clear();

        for (int id = 1; id <= 5; id++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().payload("" + id).topic("mytopic").type("mytype").build());
        }

        final List<KfkaMessage> received = new LinkedList<>();

        // Rewind 2 messages
        kfkaManager.addListener(received::add, "000003");

        // New message
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage").topic("mytopic").type("mytype").build());

        //assertThat(received).hasSize(3);
        assertThat(received.get(0).getMessageId()).isEqualTo("000004");
        assertThat(received.get(1).getMessageId()).isEqualTo("000005");
        assertThat(received.get(2).getMessageId()).isEqualTo("000006");
    }

    @Test
    void testQuerySinceBeginningFilteredByTopic()
    {
        kfkaManager.clear();

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").build());

        final List<KfkaMessage> received = new LinkedList<>();
        final KfkaPredicate predicate = new KfkaPredicate().topic("bar");
        final int seen = kfkaManager.addListener(received::add, predicate, 100);
        assertThat(seen).isEqualTo(2);

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getMessageId()).isEqualTo("000002");
        assertThat(received.get(1).getMessageId()).isEqualTo("000004");
    }

    @Test
    void testOnlyNewMessages()
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
        assertThat(received.get(0).getMessageId()).isEqualTo("000005");
    }

    @Test
    void testOnlyNewMessagesWhenManyOld()
    {
        kfkaManager.clear();

        for (int i = 0; i < 10_000; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().payload("oldMessage" + (i + 1)).topic("foo").type("mytype").build());
        }

        final KfkaPredicate predicate = new KfkaPredicate().topic("foo");
        final List<KfkaMessage> received = new LinkedList<>();
        kfkaManager.addListener(received::add, predicate, 10);

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("Message").topic("foo").type("mytype").build());

        assertThat(received).hasSize(11);
        assertThat(received.get(0).getMessageId()).isEqualTo("009991");
    }

    @Test
    void testCleanOldMessages()
    {
        final String type = "mytype";

        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type(type).build());

        final List<KfkaMessage> messages = new LinkedList<>();

        kfkaManager.addListener(messages::add, new KfkaPredicate(), 100);
        assertThat(messages).hasSize(4);

        messages.clear();
        kfkaManager.clear();

        kfkaManager.addListener(messages::add, new KfkaPredicate(), 100);
        assertThat(messages).isEmpty();
    }

    @Test
    void testSize()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        assertThat(kfkaManager.size()).isEqualTo(2);
    }

    @Test
    void testQueryWithRelativeOffsetFilteredByTopic()
    {
        final String type = "my_type";

        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type(type).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type(type).build());

        final List<KfkaMessage> received = new LinkedList<>();
        kfkaManager.addListener(received::add, new KfkaPredicate().topic("bar"), 1);

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getMessageId()).isEqualTo("000004");
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopicAndCustomProperty() throws InterruptedException
    {
        kfkaManager.clear();

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(321).payload("myMessage1").topic("bar").type("mytype").build());

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage2").topic("bar").type("mytype").build());

        final CollectingListener<CustomKfkaMessage> collListener = new CollectingListener<>();
        kfkaManager.addListener(collListener, new KfkaPredicate().topic("bar").addPropertyMatch("userId", 123), 10);

        assertThat(collListener.getReceived()).hasSize(1);
        assertThat(collListener.getReceived().get(0).getMessageId()).isEqualTo("000002");

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage3").topic("bar").type("mytype").build());

        kfkaManager.add(new CustomKfkaMessageBuilder().userId(321).payload("myMessage4").topic("bar").type("mytype").build());

        // Need to allow asynchronous messages propagate
        Thread.sleep(100);

        assertThat(collListener.getReceived()).hasSize(2);
        assertThat(collListener.getReceived().get(1).getMessageId()).isEqualTo("000003");
    }

    @Test
    void testLastMessageId()
    {
        kfkaManager.clear();
        final CustomKfkaMessage msg1 = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").payload("msg1").type("mytype").build());

        final CustomKfkaMessage msg2 = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").payload("msg2").build());

        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.addListener(l, new KfkaPredicate(), msg1.getMessageId());
        assertThat(l.getReceived()).containsOnly(msg2);
    }

    @Test
    void testMessagesPersisted()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.addListener(l, new KfkaPredicate(), 1);
        assertThat(l.getReceived()).hasSize(1);
    }

    @Test
    void testMessagesExpire()
    {
        kfkaManager.clear();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").timestamp(OffsetDateTime.parse("2000-01-01T00:00:00Z")).type("mytype").build());
        final CollectingListener<CustomKfkaMessage> l = new CollectingListener<>();
        kfkaManager.evictExpired();
        kfkaManager.addListener(l, new KfkaPredicate(), 1_000);
        assertThat(l.getReceived()).isEmpty();
    }

    @Test
    void testPerformance1()
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

        sw.start("Insert3");
        final List<CustomKfkaMessage> messages = new ArrayList<>(count);
        for (int i = 1; i <= count; i++)
        {
            messages.add(new CustomKfkaMessageBuilder().userId(123).payload("myMessage" + 1).topic("bar").type("mytype").build());
        }
        kfkaManager.addAll(messages);
        sw.stop();

        sw.start("Query");
        final CollectingListener<CustomKfkaMessage> collListener = new CollectingListener<>();
        kfkaManager.addListener(collListener, new KfkaPredicate().topic("bar").addPropertyMatch("userId", 123), count);
        sw.stop();
        assertThat(collListener.getReceived()).hasSize(count);
        assertThat(collListener.getReceived().get(0).getMessageId()).isEqualTo("200001");
        assertThat(collListener.getReceived().get(count - 1).getMessageId()).isEqualTo(Long.toString(count * 3));
        logger.info("Timings: {}", sw);
    }
}
