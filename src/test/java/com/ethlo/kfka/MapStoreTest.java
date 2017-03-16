package com.ethlo.kfka;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaMapStore;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class)
@Transactional
public class MapStoreTest
{
    private static final Logger logger = LoggerFactory.getLogger(KfkaApplicationTests.class);
    
    @Autowired
    private KfkaMapStore<CustomKfkaMessage> mapStore;
    
    @Test
    public void testStoreAll()
    {
        final Map<Long, CustomKfkaMessage> input = new TreeMap<>();
        input.put(123L, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("hello").id(123L).build());
        input.put(129L, new CustomKfkaMessage.CustomKfkaMessageBuilder().topic("foobar").type("mytype").payload("goodbye").id(129L).build());
        mapStore.storeAll(input);
        
        final Map<Long, CustomKfkaMessage> output = mapStore.loadAll(input.keySet());
        assertThat(output).isEqualTo(input);
    }
}
