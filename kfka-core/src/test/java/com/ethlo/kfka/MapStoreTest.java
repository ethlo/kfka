package com.ethlo.kfka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaMessageStore;

public abstract class MapStoreTest
{
    @Autowired
    private KfkaMessageStore mapStore;

    @Test
    public void testStoreAll()
    {
        mapStore.add(new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(123L).build());
        mapStore.add(new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(129L).build());
        //final List<CustomKfkaMessage> output = mapStore.loadAll();
        //assertThat(output).isEqualTo(input);
    }

    @Test
    public void testStoreSingle()
    {
        final CustomKfkaMessage message = new CustomKfkaMessage.CustomKfkaMessageBuilder()
                .topic("foobar")
                .type("mytype")
                .payload("hello")
                .id(130L).build();
        mapStore.add(message);
        assertThat(mapStore.head().next()).isEqualTo(message);
    }
}
