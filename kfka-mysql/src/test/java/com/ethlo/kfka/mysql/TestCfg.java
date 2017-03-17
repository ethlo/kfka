package com.ethlo.kfka.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

/*-
 * #%L
 * kfka-mysql
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

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.KfkaConfig;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaManagerImpl;
import com.ethlo.kfka.persistence.KfkaCounterStore;
import com.ethlo.kfka.persistence.KfkaMapStore;
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
    public static Flyway flyway(DataSource ds)
    {
        final Flyway flyway = new Flyway();
        flyway.setLocations("db/testmigrations");
        flyway.setDataSource(ds);
        flyway.migrate();
        return flyway;
    }
    
    @Bean
    public KfkaCounterStore counterStore(DataSource ds)
    {
        return new MysqlKfkaCounterStore(ds);
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
        
        return new MysqlKfkaMapStore<CustomKfkaMessage>(ds, ROW_MAPPER);
    }
    
    @Bean
    public KfkaManager kfkaConsumer(HazelcastInstance hazelcastInstance, KfkaMapStore<CustomKfkaMessage> mapStore, KfkaCounterStore counterStore)
    {
        return new KfkaManagerImpl(hazelcastInstance, mapStore, counterStore, 
            new KfkaConfig()
                .ttl(300, TimeUnit.SECONDS)
                .name("kfka")
                .writeDelay(0)
                .persistent(true)
                .initialLoadMode(InitialLoadMode.EAGER)
                .batchSize(250));
    }
}