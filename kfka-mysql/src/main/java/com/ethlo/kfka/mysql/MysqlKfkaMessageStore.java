package com.ethlo.kfka.mysql;

/*-
 * #%L
 * kfka-mysql
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
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.KfkaPredicate;
import com.ethlo.kfka.UnknownMessageIdException;
import com.ethlo.kfka.persistence.KfkaMessageStore;
import com.ethlo.kfka.util.AbstractIterator;
import com.ethlo.kfka.util.ReflectionUtil;

public class MysqlKfkaMessageStore<B extends KfkaMessage> implements KfkaMessageStore
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlKfkaMessageStore.class);

    private final RowMapper<B> mapper;
    private final Duration ttl;

    private final DataSource dataSource;
    private final SimpleJdbcTemplate simpleTpl;

    public MysqlKfkaMessageStore(DataSource dataSource, RowMapper<B> mapper, Duration ttl)
    {
        this.dataSource = dataSource;
        this.mapper = mapper;
        this.ttl = ttl;
        this.simpleTpl = new SimpleJdbcTemplate(dataSource);
    }

    private long getTtlTs()
    {
        return System.currentTimeMillis() - ttl.toMillis();
    }

    private <T extends KfkaMessage> AbstractIterator<T> fromMessageIdIterator(final String lastSeenMessageId, final KfkaPredicate predicate)
    {
        final Long internalId = simpleTpl.queryForObject("SELECT id FROM kfka WHERE message_id = ?", Collections.singletonList(lastSeenMessageId), Long.class);
        if (internalId != null)
        {
            return fromMessageIdIterator(internalId, predicate);
        }

        throw new UnknownMessageIdException(lastSeenMessageId);
    }

    private <T extends KfkaMessage> AbstractIterator<T> fromMessageIdIterator(final Long lastSeenMessageId, final KfkaPredicate predicate)
    {
        final List<Object> params = new LinkedList<>();
        final StringBuilder sql = new StringBuilder("SELECT * FROM kfka WHERE 1=1");
        if (lastSeenMessageId != null)
        {
            sql.append(" AND id >= ?");
        }
        sql.append(" AND timestamp > ?");
        addFilterPredicates(predicate, params, sql);
        sql.append(" ORDER BY id ASC");

        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        try
        {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            int index = 1;
            if (lastSeenMessageId != null)
            {
                stmt.setLong(index++, lastSeenMessageId);
            }
            stmt.setLong(index++, getTtlTs());

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
                        return (T) mapper.mapRow(rs);
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
    public <T extends KfkaMessage> T add(T value)
    {
        final List<Object> params = getInsertParams(value);
        final String sql = getInsertSql(value);
        final long newId = simpleTpl.insert(sql, params);
        value.setId(newId);
        return value;
    }

    private <T extends KfkaMessage> String getInsertSql(T value)
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
        return this.simpleTpl.queryForObject("SELECT COUNT(id) FROM kfka", Collections.emptyList(), Long.class);
    }

    private List<Object> getInsertParams(KfkaMessage value)
    {
        final List<Object> result = new LinkedList<>();
        result.add(value.getMessageId());
        result.add(value.getType());
        result.add(value.getTopic());
        result.add(value.getTimestamp());
        result.add(value.getPayload());

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
    public void sendAfter(final String messageId, final KfkaPredicate predicate, final KfkaMessageListener l)
    {
        try (final AbstractIterator<KfkaMessage> iter = fromMessageIdIterator(messageId, predicate))
        {
            if (iter.hasNext())
            {
                // Skip self
                iter.next();
            }
            while (iter.hasNext())
            {
                final KfkaMessage msg = iter.next();
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
        params.add(System.currentTimeMillis() - ttl.toMillis());
        addFilterPredicates(predicate, params, sql);
        sql.append(" ORDER BY id DESC");
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
    public void sendAll(final KfkaPredicate predicate, final KfkaMessageListener l)
    {
        try (final AbstractIterator<KfkaMessage> iter = fromMessageIdIterator((Long)null, predicate))
        {
            while (iter.hasNext())
            {
                final KfkaMessage msg = iter.next();
                l.onMessage(msg);
            }
        }
    }

    @Override
    public void sendIncluding(final String messageId, final KfkaPredicate predicate, final KfkaMessageListener l)
    {
        try (final AbstractIterator<KfkaMessage> iter = fromMessageIdIterator(messageId, predicate))
        {
            while (iter.hasNext())
            {
                final KfkaMessage msg = iter.next();
                l.onMessage(msg);
            }
        }
    }
}

