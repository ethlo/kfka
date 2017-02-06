package com.ethlo.kfka;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.sql.SqlKfkaCounterStore;
import com.ethlo.kfka.sql.SqlKfkaMapStore;
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
    public KfkaMapStore<CustomKfkaMessage> mapStore(DataSource ds)
    {
        final RowMapper<CustomKfkaMessage> ROW_MAPPER = new RowMapper<CustomKfkaMessage>()
        {
            @Override
            public CustomKfkaMessage mapRow(ResultSet rs, int rowNum) throws SQLException
            {
                return new CustomKfkaMessageBuilder()
                     .userId(rs.getInt("userId"))
                     .payload(rs.getBytes("payload"))
                     .timestamp(rs.getLong("timestamp"))
                     .topic(rs.getString("topic"))
                     .type(rs.getString("type"))
                     .id(rs.getLong("id"))
                     .build();
            }
        };
        
        return new SqlKfkaMapStore<CustomKfkaMessage>(ds, ROW_MAPPER);
    }
    
    @Bean
    public KfkaManager kfkaConsumer(HazelcastInstance hazelcastInstance, KfkaMapStore<CustomKfkaMessage> mapStore, KfkaCounterStore counterStore)
    {
        return new KfkaManagerImpl(hazelcastInstance, mapStore, counterStore);
    }
}
