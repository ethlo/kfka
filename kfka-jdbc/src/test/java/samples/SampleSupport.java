package samples;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaManagerImpl;
import com.ethlo.kfka.jdbc.JdbcKfkaMessageStore;
import com.ethlo.kfka.jdbc.RowMapper;

/**
 * Shared setup for the README samples. Kept outside the sample directories so none of it is extracted.
 */
public final class SampleSupport
{
    private SampleSupport()
    {
    }

    /**
     * A fresh in-memory H2 database with the kfka table from the test migrations.
     *
     * @return the data source
     */
    public static DataSource dataSource()
    {
        final JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");
        try (InputStream in = SampleSupport.class.getResourceAsStream("/db/test_migrations/V1.0.0__initial.sql");
             Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement())
        {
            stmt.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
        catch (SQLException exc)
        {
            throw new IllegalStateException(exc);
        }
        return dataSource;
    }

    /**
     * A manager set up like the configuration sample, on a fresh database.
     *
     * @return the manager
     */
    public static KfkaManager<CustomKfkaMessage> kfkaManager()
    {
        final RowMapper<CustomKfkaMessage> rowMapper = rs -> new CustomKfkaMessageBuilder()
                .userId(rs.getInt("userId"))
                .payload(rs.getBytes("payload"))
                .timestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong("timestamp")), ZoneOffset.UTC))
                .topic(rs.getString("topic"))
                .type(rs.getString("type"))
                .messageId(rs.getString("message_id"))
                .build();
        final AtomicLong lastId = new AtomicLong();
        return new KfkaManagerImpl<>(new JdbcKfkaMessageStore<>(dataSource(), rowMapper, Duration.ofDays(3)),
                () -> String.format("%020d", lastId.incrementAndGet()));
    }

    /**
     * Publish a text message on a topic for a user.
     *
     * @param kfkaManager the manager
     * @param topic       the topic
     * @param userId      the user
     * @return the message, with its id set
     */
    public static CustomKfkaMessage send(KfkaManager<CustomKfkaMessage> kfkaManager, String topic, int userId)
    {
        return kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(userId)
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .topic(topic)
                .type("text")
                .build());
    }
}
