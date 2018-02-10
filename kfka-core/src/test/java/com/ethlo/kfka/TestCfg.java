package com.ethlo.kfka;

import java.time.Duration;

/*-
 * #%L
 * kfka-core
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

import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import com.acme.CustomKfkaMessage;
import com.ethlo.kfka.persistence.KfkaCounterStore;
import com.ethlo.kfka.persistence.KfkaMapStore;
import com.ethlo.kfka.persistence.MemoryKfkaCounterStore;
import com.ethlo.kfka.persistence.MemoryKfkaMapStore;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Configuration
@EnableAutoConfiguration
public class TestCfg
{
    @Bean(destroyMethod="shutdown")
    public HazelcastInstance hazelcastInstance()
    {
        return Hazelcast.newHazelcastInstance();
    }
    
    @Bean
    public KfkaCounterStore counterStore()
    {
        return new MemoryKfkaCounterStore();
    }
    
    @Scheduled(fixedDelay=60_000)
    public void cleanup(KfkaManager kfkaManager)
    {
        kfkaManager.cleanExpired();
    }
    
    @Bean
    public KfkaMapStore<CustomKfkaMessage> mapStore()
    {
        return new MemoryKfkaMapStore<CustomKfkaMessage>(Duration.ofSeconds(300));
    }
    
    @Bean
    public KfkaManager kfkaConsumer(HazelcastInstance hazelcastInstance, KfkaMapStore<CustomKfkaMessage> mapStore, KfkaCounterStore counterStore)
    {
        return new KfkaManagerImpl(hazelcastInstance, mapStore, counterStore, 
            new KfkaConfig()
                .ttl(Duration.ofSeconds(300))
                .name("kfka")
                .writeDelay(0)
                .persistent(true)
                .initialLoadMode(InitialLoadMode.EAGER)
                .batchSize(250));
    }
}
