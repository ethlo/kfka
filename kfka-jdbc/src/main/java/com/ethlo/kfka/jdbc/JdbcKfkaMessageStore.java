package com.ethlo.kfka.jdbc;

/*-
 * #%L
 * kfka-jdbc
 * %%
 * Copyright (C) 2017 - 2018 Morten Haraldsen (ethlo)
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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.KfkaPredicate;
import com.ethlo.kfka.UnknownMessageIdException;
import com.ethlo.kfka.compression.NopPayloadCompressor;
import com.ethlo.kfka.compression.PayloadCompressor;
import com.ethlo.kfka.persistence.KfkaMessageStore;
import com.ethlo.kfka.util.AbstractIterator;
import com.ethlo.kfka.util.ReflectionUtil;

public class JdbcKfkaMessageStore<T extends KfkaMessage> implements KfkaMessageStore<T>
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcKfkaMessageStore.class);

    private final RowMapper<T> mapper;
    private final Duration ttl;
    private final DataSource dataSource;
    private final SimpleJdbcTemplate simpleTpl;
    private final PayloadCompressor payloadCompressor;
    private final int batchSize;

    public JdbcKfkaMessageStore(DataSource dataSource, RowMapper<T> mapper, Duration ttl)
    {
        this(dataSource, mapper, ttl, new NopPayloadCompressor(), 1_000);
    }

    public JdbcKfkaMessageStore(DataSource dataSource, RowMapper<T> mapper, Duration ttl, final PayloadCompressor payloadCompressor, final int batchSize)
    {
        this.dataSource = dataSource;
        this.mapper = mapper;
        this.ttl = ttl;
        this.payloadCompressor = payloadCompressor;
        this.simpleTpl = new SimpleJdbcTemplate(dataSource);
        this.batchSize = batchSize;
    }

    private long getTtlTs()
    {
        return System.currentTimeMillis() - ttl.toMillis();
    }

    private AbstractIterator<T> fromMessageIdIterator(final String lastSeenMessageId, final KfkaPredicate predicate)
    {
        final long ttlTs = getTtlTs();

        if (lastSeenMessageId != null && findMessage(lastSeenMessageId, ttlTs).isEmpty())
        {
            throw new UnknownMessageIdException(lastSeenMessageId);
        }

        final List<Object> params = new ArrayList<>();
        final StringBuilder sql = new StringBuilder("SELECT * FROM kfka");

        // Filter out too old
        sql.append(" WHERE timestamp > ?");

        // Continue from this message
        if (lastSeenMessageId != null)
        {
            sql.append(" AND message_id >= ?");
        }

        addFilterPredicates(predicate, params, sql);

        sql.append(" ORDER BY message_id");

        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        try
        {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            int index = 1;

            stmt.setLong(index++, ttlTs);

            if (lastSeenMessageId != null)
            {
                stmt.setString(index++, lastSeenMessageId);
            }

            for (int i = 0; i < params.size(); i++)
            {
                stmt.setString(i + index, params.get(i).toString());
            }
            rs = stmt.executeQuery();
        }
        catch (SQLException exc)
        {
            throw new RuntimeSqlException(exc);
        }

        return new AbstractIterator<T>()
        {
            @Override
            protected T computeNext()
            {
                try
                {
                    if (rs.next())
                    {
                        final T e = mapper.mapRow(rs);
                        e.setPayload(payloadCompressor.decompress(e.getPayload()));
                        logger.trace("Returning: {}", e);
                        return e;
                    }
                    else
                    {
                        logger.info("Exhausted iterator");
                    }
                }
                catch (SQLException exc)
                {
                    throw new RuntimeSqlException(exc);
                }

                return endOfData();
            }

            @Override
            public void close()
            {
                logger.debug("Closing iterator");
                try
                {
                    rs.close();
                    stmt.close();
                    conn.close();
                }
                catch (SQLException exc)
                {
                    throw new RuntimeSqlException(exc);
                }
            }
        };
    }

    private Optional<byte[]> findMessage(String lastSeenMessageId, long ttlTs)
    {
        return simpleTpl.queryForObject("""
                SELECT message_id
                FROM kfka
                WHERE message_id = ?
                AND timestamp > ?""", List.of(lastSeenMessageId, ttlTs), byte[].class);
    }

    private long countAvailableAfter(String lastSeenMessageId)
    {
        return simpleTpl.queryForObject("""
                SELECT count(1)
                FROM kfka
                WHERE message_id >= ?
                AND timestamp > ?""", List.of(lastSeenMessageId, getTtlTs()), Long.class).orElseThrow();
    }

    private void addFilterPredicates(KfkaPredicate predicate, List<Object> params, StringBuilder sql)
    {
        Optional.ofNullable(predicate.getType()).ifPresent(p ->
        {
            sql.append(" AND ").append("type").append(" = ?");
            params.add(p);
        });

        Optional.ofNullable(predicate.getTopic()).ifPresent(p ->
        {
            sql.append(" AND ").append("topic").append(" = ?");
            params.add(p);
        });

        final Map<String, Serializable> propertyMatches = predicate.getPropertyMatch();
        for (final Map.Entry<String, Serializable> e : propertyMatches.entrySet())
        {
            sql.append(" AND ").append(e.getKey()).append(" = ?");
            params.add(e.getValue());
        }
    }

    @Override
    public void addAll(List<T> values)
    {
        if (values.isEmpty())
        {
            return;
        }

        final T value = values.get(0);
        final String sql = getInsertSql(value);

        final BiConsumer<T, SimpleJdbcTemplate.ParameterSink> sinkConsumer = (v, sink) ->
        {
            final List<Object> params = getInsertParams(v);
            for (int i = 0; i < params.size(); i++)
            {
                sink.setObject(i + 1, params.get(i));
            }
        };

        simpleTpl.insertBatch(sql, values.iterator(), sinkConsumer, batchSize);
    }

    private String getInsertSql(T value)
    {
        final Collection<String> extraProps = value.getQueryableProperties();
        final String extraColsStr = (extraProps.isEmpty() ? "" : (", " + collectionToCommaDelimitedString(extraProps)));
        return "INSERT INTO kfka (message_id, type, topic, timestamp, payload" + extraColsStr + ")"
                + " VALUES(" + repeat(5 + extraProps.size()) + ")";
    }

    private String collectionToCommaDelimitedString(Collection<?> props)
    {
        return props.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    private String repeat(int size)
    {
        final List<String> tmp = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            tmp.add("?");
        }
        return collectionToCommaDelimitedString(tmp);
    }

    @Override
    public long size()
    {
        return this.simpleTpl.queryForObject("SELECT COUNT(1) FROM kfka", Collections.emptyList(), Long.class).orElseThrow();
    }

    private List<Object> getInsertParams(KfkaMessage value)
    {
        final List<Object> result = new LinkedList<>();
        result.add(value.getMessageId());
        result.add(value.getType());
        result.add(value.getTopic());
        result.add(value.getTimestamp().toInstant().toEpochMilli());
        result.add(payloadCompressor.compress(value.getPayload()));

        for (String propName : value.getQueryableProperties())
        {
            result.add(ReflectionUtil.getPropertyValue(value, propName));
        }

        return result;
    }

    @Override
    public void clear()
    {
        simpleTpl.update("TRUNCATE TABLE kfka", Collections.emptyList());
    }

    @Override
    public void sendAfter(final String messageId, final KfkaPredicate predicate, final KfkaMessageListener<T> l)
    {
        try (final AbstractIterator<T> iter = fromMessageIdIterator(messageId, predicate))
        {
            if (iter.hasNext())
            {
                // Skip self
                iter.next();
            }

            while (iter.hasNext())
            {
                final T msg = iter.next();
                l.onMessage(msg);
            }
        }
    }

    @Override
    public Optional<String> getMessageIdForRewind(final KfkaPredicate predicate)
    {
        final int rewind = predicate.getRewind();
        final StringBuilder sql = new StringBuilder("SELECT message_id FROM kfka WHERE timestamp > ?");
        final List<Object> params = new LinkedList<>();
        params.add(getTtlTs());
        addFilterPredicates(predicate, params, sql);
        sql.append(" ORDER BY message_id DESC");
        return simpleTpl.query(sql.toString(), params, rs ->
        {
            String firstFound = null;
            int count = 0;
            try
            {
                while (rs.next() && count++ < rewind)
                {
                    firstFound = rs.getString(1);
                }
            }
            catch (SQLException exc)
            {
                throw new RuntimeSqlException(exc);
            }

            return Optional.ofNullable(firstFound);
        });
    }

    @Override
    public void clearExpired()
    {
        simpleTpl.update("DELETE FROM kfka WHERE timestamp < ?", Collections.singletonList(System.currentTimeMillis() - ttl.toMillis()));
    }

    @Override
    public void sendAll(final KfkaPredicate predicate, final KfkaMessageListener<T> l)
    {
        try (final AbstractIterator<T> iter = fromMessageIdIterator(null, predicate))
        {
            while (iter.hasNext())
            {
                final T msg = iter.next();
                l.onMessage(msg);
            }
        }
    }

    @Override
    public Optional<String> getLastKnownId()
    {
        return simpleTpl.queryForObject("SELECT MAX(message_id) FROM kfka", List.of(), String.class);
    }

    @Override
    public void sendIncluding(final String messageId, final KfkaPredicate predicate, final KfkaMessageListener<T> l)
    {
        try (final AbstractIterator<T> iter = fromMessageIdIterator(messageId, predicate))
        {
            while (iter.hasNext())
            {
                final T msg = iter.next();
                l.onMessage(msg);
            }
        }
    }
}

