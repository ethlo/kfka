package com.ethlo.kfka.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.h2.util.StringUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaManagerImpl;
import com.ethlo.kfka.compression.GzipPayloadCompressor;
import com.ethlo.kfka.persistence.KfkaMessageStore;

/*-
 * #%L
 * kfka-jdbc
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

@Configuration
@EnableAutoConfiguration
public class TestCfg
{
    @Bean
    public static Flyway flyway(DataSource ds)
    {
        final Flyway flyway = Flyway.configure().locations("db/test_migrations").dataSource(ds).load();
        flyway.migrate();
        return flyway;
    }

    @Bean
    public KfkaMessageStore<CustomKfkaMessage> mapStore(DataSource ds)
    {
        final Duration ttl = Duration.ofMinutes(30);
        final RowMapper<CustomKfkaMessage> ROW_MAPPER = rs ->
                new CustomKfkaMessageBuilder()
                        .userId(rs.getInt("userId"))
                        .payload(rs.getBytes("payload"))
                        .timestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong("timestamp")), ZoneOffset.UTC))
                        .topic(rs.getString("topic"))
                        .type(rs.getString("type"))
                        .messageId(rs.getString("message_id"))
                        .build();

        return new JdbcKfkaMessageStore<>(ds, ROW_MAPPER, ttl, new GzipPayloadCompressor(), 10_000)
        {
        };
    }

    @Bean
    public KfkaManager<CustomKfkaMessage> kfkaManager(KfkaMessageStore<CustomKfkaMessage> messageStore)
    {
        final AtomicLong idSource = new AtomicLong();
        return new KfkaManagerImpl<>(messageStore, () -> StringUtils.pad(Long.toString(idSource.incrementAndGet()), 6, "0", false));
    }
}