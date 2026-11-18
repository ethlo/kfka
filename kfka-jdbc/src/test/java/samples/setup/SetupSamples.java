package samples.setup;

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

import static org.assertj.core.api.Assertions.assertThat;
import static samples.SampleSupport.dataSource;
import static samples.SampleSupport.kfkaManager;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import com.acme.CustomKfkaMessage;
import com.acme.CustomKfkaMessage.CustomKfkaMessageBuilder;
import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaManagerImpl;
import com.ethlo.kfka.MessageIdGenerator;
import com.ethlo.kfka.jdbc.JdbcKfkaMessageStore;
import com.ethlo.kfka.jdbc.RowMapper;
import com.ethlo.kfka.persistence.KfkaMessageStore;

class SetupSamples
{
    /**
     * Store messages in a database table, keep them for 3 days, and evict expired messages every 30 minutes.
     * Message ids must sort in the order the messages were added, so this generator is based on the clock and
     * strictly increasing.
     */
    @Test
    void configuration()
    {
        final Duration retentionTime = Duration.ofDays(3);
        final Duration cleanInterval = Duration.ofMinutes(30);

        final DataSource dataSource = dataSource();
        final RowMapper<CustomKfkaMessage> rowMapper = rs -> new CustomKfkaMessageBuilder()
                .userId(rs.getInt("userId"))
                .payload(rs.getBytes("payload"))
                .timestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong("timestamp")), ZoneOffset.UTC))
                .topic(rs.getString("topic"))
                .type(rs.getString("type"))
                .messageId(rs.getString("message_id"))
                .build();

        final AtomicLong lastId = new AtomicLong();
        final MessageIdGenerator idGenerator = () -> String.format("%020d",
                lastId.updateAndGet(previous -> Math.max(previous + 1, System.currentTimeMillis() * 1_000)));

        final KfkaMessageStore<CustomKfkaMessage> msgStore =
                new JdbcKfkaMessageStore<>(dataSource, rowMapper, retentionTime);
        final KfkaManager<CustomKfkaMessage> kfkaManager = new KfkaManagerImpl<>(msgStore, idGenerator);

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final long interval = cleanInterval.toMillis();
        scheduler.scheduleAtFixedRate(kfkaManager::evictExpired, interval, interval, TimeUnit.MILLISECONDS);

        assertThat(kfkaManager.size()).isZero();
        scheduler.shutdownNow();
    }

    /**
     * Publish a message. The manager assigns the message id, and the timestamp if it has none.
     */
    @Test
    void publish()
    {
        final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();

        final CustomKfkaMessage msg = kfkaManager.add(new CustomKfkaMessageBuilder()
                .userId(123)
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .topic("chat")
                .type("text")
                .build());

        assertThat(msg.getMessageId()).isNotNull();
        assertThat(kfkaManager.size()).isEqualTo(1);
    }
}
