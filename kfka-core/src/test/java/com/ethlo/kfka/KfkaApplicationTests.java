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

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class)
public class KfkaApplicationTests
{
    private final Logger logger = LoggerFactory.getLogger(KfkaApplicationTests.class);

    @Autowired
    private KfkaManager kfkaManager;

    @Test
    public void contextLoads()
    {
        assertThat(true).isTrue();
    }

    @Test
    public void testQueryLast() throws InterruptedException
    {
        kfkaManager.clearAll();

        for (int i = 0; i < 1_000; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder().payload("" + i).topic("mytopic").type("mytype").build());
        }

        final List<KfkaMessage> received = new LinkedList<>();

        kfkaManager.addListener(received::add, new KfkaPredicate().relativeOffset(-1));

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage").topic("mytopic").type("mytype").build());

        Thread.sleep(100);

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getPayload()).isEqualTo("999".getBytes());
        assertThat(received.get(1).getPayload()).isEqualTo("myMessage".getBytes());
    }

    @Test
    public void testQuerySinceBeginningFilteredByTopic()
    {
        kfkaManager.clearAll();

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").build());

        final List<KfkaMessage> received = new LinkedList<>();
        final KfkaPredicate predicate = new KfkaPredicate().topic("bar").messageId(0);
        this.kfkaManager.addListener(received::add, predicate);

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getId()).isEqualTo(2);
        assertThat(received.get(1).getId()).isEqualTo(4);
    }

    @Test
    public void testCleanOldMessages()
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").timestamp(1).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").timestamp(2).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").timestamp(3).build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").timestamp(4).build());

        final List<KfkaMessage> messages = new LinkedList<>();

        kfkaManager.addListener(messages::add, new KfkaPredicate().relativeOffset(-100));
        assertThat(messages).hasSize(4);

        kfkaManager.loadAll();
        messages.clear();

        kfkaManager.addListener(messages::add, new KfkaPredicate().relativeOffset(-100));
        assertThat(messages).isEmpty();
    }

    @Test
    public void testSize()
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        assertThat(kfkaManager.size()).isEqualTo(2);
    }

    @Test
    public void testFindFirst()
    {
        kfkaManager.clearAll();
        final long a = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        final long first = kfkaManager.findfirst();
        assertThat(first).isEqualTo(a);
    }

    @Test
    public void testFindLatest()
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        final long b = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        final long latest = kfkaManager.findLatest();
        assertThat(latest).isEqualTo(b);
    }

    @Test
    public void testDeleteMessages()
    {
        kfkaManager.clearAll();
        final long id = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        final long left = kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.delete(id);
        assertThat(kfkaManager.size()).isEqualTo(1);
        assertThat(kfkaManager.findfirst()).isEqualTo(left);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopic()
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").topic("bar").type("mytype").build());

        final List<KfkaMessage> received = new LinkedList<>();
        kfkaManager.addListener(received::add, new KfkaPredicate().topic("bar").relativeOffset(-1));

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(4);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopicAndCustomProperty() throws InterruptedException
    {
        kfkaManager.clearAll();

        kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(321)
                .payload("myMessage1")
                .topic("bar")
                .type("mytype")
                .build());

        kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(123)
                .payload("myMessage2")
                .topic("bar")
                .type("mytype")
                .build());

        final CollectingListener collListener = new CollectingListener();
        kfkaManager.addListener(collListener, new KfkaPredicate()
                .topic("bar")
                .relativeOffset(-10)
                .addPropertyMatch("userId", 123));

        assertThat(collListener.getReceived()).hasSize(1);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(2);

        kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(123)
                .payload("myMessage3")
                .topic("bar")
                .type("mytype")
                .build());

        kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(321)
                .payload("myMessage4")
                .topic("bar")
                .type("mytype")
                .build());

        // Need to allow asynchronous messages propagate
        Thread.sleep(100);

        assertThat(collListener.getReceived()).hasSize(2);
        assertThat(collListener.getReceived().get(1).getId()).isEqualTo(3);
    }

    @Test
    public void testLastMessageId()
    {
        kfkaManager.clearAll();
        final CustomKfkaMessage msg1 = new CustomKfkaMessageBuilder().payload("myMessage1")
                .topic("foo")
                .payload("msg1")
                .type("mytype")
                .build();

        final CustomKfkaMessage msg2 = new CustomKfkaMessageBuilder().payload("myMessage1")
                .topic("foo")
                .type("mytype")
                .payload("msg2")
                .build();

        kfkaManager.add(msg1);
        kfkaManager.add(msg2);
        final CollectingListener l = new CollectingListener();
        kfkaManager.addListener(l, new KfkaPredicate().messageId(msg2.getId()));
        assertThat(l.getReceived()).hasSize(1);
        assertThat(l.getReceived()).contains(msg2);
    }

    @Test
    public void testMessagesPersisted() throws InterruptedException
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1")
                .topic("foo")
                .type("mytype").build());
        kfkaManager.clearCache();
        Thread.sleep(1500);
        long count = kfkaManager.loadAll();
        assertThat(count).isEqualTo(1);
        final CollectingListener l = new CollectingListener();
        kfkaManager.addListener(l, new KfkaPredicate().messageId(0));
        assertThat(l.getReceived()).hasSize(1);
    }

    @Test
    public void testPerformance1()
    {
        kfkaManager.clearAll();

        final int count = 10_000;
        final StopWatch sw = new StopWatch();
        sw.start("Insert1");
        for (int i = 1; i <= count; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder()
                    .userId(321)
                    .payload("otherMessage" + i)
                    .topic("bar")
                    .type("mytype")
                    .build());
        }
        sw.stop();

        sw.start("Insert2");
        for (int i = 1; i <= count; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder()
                    .userId(123)
                    .payload("myMessage" + 1)
                    .topic("bar")
                    .type("mytype")
                    .build());
        }
        sw.stop();

        sw.start("Query");
        final CollectingListener collListener = new CollectingListener();
        kfkaManager.addListener(collListener, new KfkaPredicate()
                .topic("bar")
                .relativeOffset(-(count + 10))
                .addPropertyMatch("userId", 123));
        sw.stop();
        assertThat(collListener.getReceived()).hasSize(count);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(count + 1);
        assertThat(collListener.getReceived().get(count - 1).getId()).isEqualTo(count + count);
        logger.info("Timings: {}", sw);
    }
}
