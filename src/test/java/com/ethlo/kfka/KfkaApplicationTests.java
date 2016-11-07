package com.ethlo.kfka;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.acme.CustomKfkaMessage;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class)
@EnableAutoConfiguration
public class KfkaApplicationTests
{
    @Autowired
    private KfkaManager kfkaManager;

    @Test
    public void contextLoads()
    {
    }

    @Test
    public void testQueryLast() throws InterruptedException
    {
        kfkaManager.clearAll();

        for (int i = 0; i < 1_000; i++)
        {
            kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("" + i).timestamp(System.currentTimeMillis()).topic("mytopic").type("mytype")));
        }

        final List<KfkaMessage> received = new LinkedList<>();

        kfkaManager.addListener(new KfkaMessageListener()
        {
            @Override
            public void onMessage(KfkaMessage msg)
            {
                received.add(msg);
            }
        }, new KfkaPredicate(kfkaManager).offset(-1));

        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage").timestamp(System.currentTimeMillis()).topic("mytopic").type("mytype")));

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getPayload()).isEqualTo("999".getBytes());
        assertThat(received.get(1).getPayload()).isEqualTo("myMessage".getBytes());
    }

    @Test
    public void testQuerySinceBeginningFilteredByTopic() throws InterruptedException
    {
        kfkaManager.clearAll();

        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage1").timestamp(System.currentTimeMillis()).topic("foo").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage2").timestamp(System.currentTimeMillis()).topic("bar").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage3").timestamp(System.currentTimeMillis()).topic("baz").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage4").timestamp(System.currentTimeMillis()).topic("bar").type("mytype")));

        final List<KfkaMessage> received = new LinkedList<>();
        new KfkaPredicate(kfkaManager).topic("bar").seekToBeginning().addListener(new KfkaMessageListener()
        {
            @Override
            public void onMessage(KfkaMessage msg)
            {
                received.add(msg);
            }
        });

        assertThat(received).hasSize(2);
        assertThat(received.get(0).getId()).isEqualTo(2);
        assertThat(received.get(1).getId()).isEqualTo(4);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopic() throws InterruptedException
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage1").timestamp(System.currentTimeMillis()).topic("foo").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage2").timestamp(System.currentTimeMillis()).topic("bar").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage3").timestamp(System.currentTimeMillis()).topic("baz").type("mytype")));
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage4").timestamp(System.currentTimeMillis()).topic("bar").type("mytype")));

        final List<KfkaMessage> received = new LinkedList<>();
        new KfkaPredicate(kfkaManager).topic("bar").offset(-1).addListener(new KfkaMessageListener()
        {
            @Override
            public void onMessage(KfkaMessage msg)
            {
                received.add(msg);
            }
        });

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(4);
    }

    @Test
    public void testQueryWithRelativeOffsetFilteredByTopicAndCustomProperties() throws InterruptedException
    {
        kfkaManager.clearAll();
        
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder().payload("myMessage1")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype"))
            .setUserId(321));
        
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder()
            .payload("myMessage2")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype"))
            .setUserId(123));
        
        final List<KfkaMessage> received = new LinkedList<>();
        new KfkaPredicate(kfkaManager)
            .topic("bar")
            .offset(-10)
            .propertyMatch(Collections.singletonMap("userId", 123))
            .addListener(new KfkaMessageListener()
            {
                @Override
                public void onMessage(KfkaMessage msg)
                {
                    received.add(msg);
                }
            });

        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(2);
        
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder()
            .payload("myMessage3")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype"))
            .setUserId(123));
        
        kfkaManager.add(new CustomKfkaMessage(new KfkaMessage.Builder()
            .payload("myMessage4")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype"))
            .setUserId(321));

        Thread.sleep(100);
        
        assertThat(received).hasSize(2);
        assertThat(received.get(1).getId()).isEqualTo(3);

    }
}
