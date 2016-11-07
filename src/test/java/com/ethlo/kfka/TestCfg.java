package com.ethlo.kfka;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Configuration
public class TestCfg
{
    @Bean
    public HazelcastInstance hazelcastInstance()
    {
        return Hazelcast.newHazelcastInstance();
    }
    
    @Bean
    public static Flyway flyway(DataSource ds)
    {
        final Flyway flyway = new Flyway();
        flyway.setDataSource(ds);
        flyway.migrate();
        return flyway;
    }
    
    @Bean
    public KfkaCounterStore counterStore(DataSource ds)
    {
        return new SqlKfkaCounterStore(ds);
    }
    
    @Bean
    public KfkaMapStore mapStore(DataSource ds)
    {
        return new SqlKfkaMapStore(ds);
    }
    
    @Bean
    public KfkaManager kfkaConsumer(HazelcastInstance hazelcastInstance, KfkaMapStore mapStore, KfkaCounterStore counterStore)
    {
        return new KfkaManagerImpl(hazelcastInstance, mapStore, counterStore);
    }
}
