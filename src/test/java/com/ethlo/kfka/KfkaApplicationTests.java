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

import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;

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
            kfkaManager.add(new CustomKfkaMessageBuilder().payload("" + i).timestamp(System.currentTimeMillis()).topic("mytopic").type("mytype").build());
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

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage").timestamp(System.currentTimeMillis()).topic("mytopic").type("mytype").build());

        Thread.sleep(100);
        
        assertThat(received).hasSize(2);
        assertThat(received.get(0).getPayload()).isEqualTo("999".getBytes());
        assertThat(received.get(1).getPayload()).isEqualTo("myMessage".getBytes());
    }

    @Test
    public void testQuerySinceBeginningFilteredByTopic() throws InterruptedException
    {
        kfkaManager.clearAll();

        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").timestamp(System.currentTimeMillis()).topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").timestamp(System.currentTimeMillis()).topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").timestamp(System.currentTimeMillis()).topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").timestamp(System.currentTimeMillis()).topic("bar").type("mytype").build());

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
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").timestamp(System.currentTimeMillis()).topic("foo").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage2").timestamp(System.currentTimeMillis()).topic("bar").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage3").timestamp(System.currentTimeMillis()).topic("baz").type("mytype").build());
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage4").timestamp(System.currentTimeMillis()).topic("bar").type("mytype").build());

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
    public void testQueryWithRelativeOffsetFilteredByTopicAndCustomProperty() throws InterruptedException
    {
        kfkaManager.clearAll();
        
        kfkaManager.add(new CustomKfkaMessageBuilder()
            .userId(321)
            .payload("myMessage1")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype")
            .build());
        
        kfkaManager.add(new CustomKfkaMessageBuilder()
            .userId(123)
            .payload("myMessage2")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype")
            .build());
        
        final CollectingListener collListener = new CollectingListener();
        new KfkaPredicate(kfkaManager)
            .topic("bar")
            .offset(-10)
            .propertyMatch(Collections.singletonMap("userId", 123))
            .addListener(collListener);

        assertThat(collListener.getReceived()).hasSize(1);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(2);
        
        kfkaManager.add(new CustomKfkaMessageBuilder()
            .userId(123)
            .payload("myMessage3")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype")
            .build());
        
        kfkaManager.add(new CustomKfkaMessageBuilder()
            .userId(321)
            .payload("myMessage4")
            .timestamp(System.currentTimeMillis())
            .topic("bar")
            .type("mytype")
            .build());

        // Need to allow asynchronous messages propagate
        Thread.sleep(100);
        
        assertThat(collListener.getReceived()).hasSize(2);
        assertThat(collListener.getReceived().get(1).getId()).isEqualTo(3);
    }
    
    @Test
    public void testMessagesPersisted() throws InterruptedException
    {
        kfkaManager.clearAll();
        kfkaManager.add(new CustomKfkaMessageBuilder().payload("myMessage1").timestamp(System.currentTimeMillis()).topic("foo").type("mytype").build());
        kfkaManager.clearCache();
        Thread.sleep(4000);
        long count = kfkaManager.loadAll();
        assertThat(count).isEqualTo(1);
        final CollectingListener l = new CollectingListener();
        kfkaManager.addListener(l, KfkaPredicate.rewind(kfkaManager, 1));
        assertThat(l.getReceived()).hasSize(1);
    }
    
    @Test
    public void testPerformance1() throws InterruptedException
    {
        kfkaManager.clearAll();
        
        final int count = 10_000;
        
        for (int i = 1; i <= count; i++)
        {
            kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(321)
                .payload("otherMessage" + i)
                .timestamp(System.currentTimeMillis())
                .topic("bar")
                .type("mytype")
                .build());
        }
        
        for (int i = 1; i <= count; i++)
        {
            
            kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(123)
                .payload("myMessage" + 1)
                .timestamp(System.currentTimeMillis())
                .topic("bar")
                .type("mytype")
                .build());
        }
            
        final CollectingListener collListener = new CollectingListener();
        new KfkaPredicate(kfkaManager)
            .topic("bar")
            .offset(-(count + 10))
            .propertyMatch(Collections.singletonMap("userId", 123))
            .addListener(collListener);

        assertThat(collListener.getReceived()).hasSize(count);
        assertThat(collListener.getReceived().get(0).getId()).isEqualTo(count + 1);
        assertThat(collListener.getReceived().get(count-1).getId()).isEqualTo(count + count);
    }
}
