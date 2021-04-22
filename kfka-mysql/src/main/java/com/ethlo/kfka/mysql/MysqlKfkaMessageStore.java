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
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.StringUtils;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.KfkaPredicate;
import com.ethlo.kfka.persistence.KfkaMessageStore;
import com.ethlo.kfka.util.AbstractIterator;
import com.ethlo.kfka.util.CloseableIterator;

public class MysqlKfkaMessageStore<B extends KfkaMessage> implements KfkaMessageStore
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlKfkaMessageStore.class);

    private final NamedParameterJdbcTemplate tpl;
    private final RowMapper<B> mapper;
    private final Duration ttl;

    private final DataSource dataSource;

    public MysqlKfkaMessageStore(DataSource dataSource, RowMapper<B> mapper, Duration ttl)
    {
        this.tpl = new NamedParameterJdbcTemplate(dataSource);
        this.dataSource = dataSource;
        this.mapper = mapper;
        this.ttl = ttl;
    }

    private long getTtlTs()
    {
        return System.currentTimeMillis() - ttl.toMillis();
    }

    @Override
    public <T extends KfkaMessage> CloseableIterator<T> tail()
    {
        return lastSeenMessageIterator(0, null);
    }

    private <T extends KfkaMessage> AbstractIterator<T> lastSeenMessageIterator(final long lastSeenMessageId, final KfkaPredicate predicate)
    {
        final List<Object> params = new LinkedList<>();
        final StringBuilder sql = new StringBuilder("SELECT * FROM kfka WHERE id > ? AND timestamp > ?");
        addFilterPredicates(predicate, params, sql);
        sql.append(" ORDER BY id ASC");

        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        try
        {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            stmt.setLong(1, lastSeenMessageId);
            stmt.setLong(2, getTtlTs());
            for (int i = 0; i < params.size(); i++)
            {
                stmt.setString(i + 3, params.get(i).toString());
            }
            rs = stmt.executeQuery();
        }
        catch (SQLException e)
        {
            throw new DataAccessResourceFailureException(e.getMessage(), e);
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
                        return (T) mapper.mapRow(rs, 0);
                    }
                }
                catch (SQLException exc)
                {
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
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
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
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
    public <T extends KfkaMessage> CloseableIterator<T> head()
    {
        return null;
    }

    @Override
    public <T extends KfkaMessage> void add(T value)
    {
        final Map<String, ?> params = getInsertParams(value);
        final String sql = getInsertSql(value);
        final KeyHolder keyHolder = new GeneratedKeyHolder();
        tpl.update(sql, new MapSqlParameterSource(params), keyHolder);
        value.setId(keyHolder.getKeyAs(Long.class));
    }

    private <T extends KfkaMessage> String getInsertSql(T value)
    {
        final Collection<String> extraProps = value.getQueryableProperties();
        final String extraColsStr = (extraProps.isEmpty() ? "" : (", " + StringUtils.collectionToCommaDelimitedString(extraProps)));
        final String extraColPlaceholdersStr = (extraProps.isEmpty() ? "" : (", :" + StringUtils.collectionToDelimitedString(extraProps, ", :")));
        return "INSERT INTO kfka (id, topic, type, timestamp, payload" + extraColsStr + ")"
                + " VALUES(:id, :topic, :type, :timestamp, :payload" + extraColPlaceholdersStr + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends KfkaMessage> void addAll(Collection<T> data)
    {
        if (data.isEmpty())
        {
            return;
        }

        final T first = data.iterator().next();
        final String sql = getInsertSql(first);
        final List<Map<String, ?>> parameters = new LinkedList<>();
        for (T entry : data)
        {
            parameters.add(getInsertParams(entry));
        }
        tpl.batchUpdate(sql, parameters.toArray(new Map[0]));
    }

    @Override
    public long size()
    {
        return tpl.queryForObject("SELECT COUNT(id) FROM kfka", Collections.emptyMap(), Long.class);
    }

    private Map<String, Object> getInsertParams(KfkaMessage value)
    {
        final Map<String, Object> retVal = new TreeMap<>();
        retVal.put("id", value.getId());
        retVal.put("payload", value.getPayload());
        retVal.put("type", value.getType());
        retVal.put("topic", value.getTopic());
        retVal.put("timestamp", value.getTimestamp());

        for (String propName : value.getQueryableProperties())
        {
            if (!retVal.containsKey(propName))
            {
                retVal.put(propName, KfkaMessage.getPropertyValue(value, propName));
            }
        }

        return retVal;
    }

    @Override
    public void clear()
    {
        tpl.update("TRUNCATE TABLE kfka", Collections.emptyMap());
    }

    @Override
    public void sendAfter(final long messageId, final KfkaPredicate predicate, final KfkaMessageListener l)
    {
        try (final AbstractIterator<KfkaMessage> iter = lastSeenMessageIterator(messageId, predicate))
        {
            while (iter.hasNext())
            {
                final KfkaMessage msg = iter.next();
                l.onMessage(msg);
            }
        }
    }

    @Override
    public Optional<Long> getOffsetMessageId(final int offset, final KfkaPredicate predicate)
    {
        final StringBuilder sql = new StringBuilder("SELECT id FROM kfka WHERE timestamp > ?");
        final List<Object> params = new LinkedList<>();
        params.add(System.currentTimeMillis() - ttl.toMillis());
        addFilterPredicates(predicate, params, sql);
        sql.append(" ORDER BY id ASC");
        return tpl.getJdbcTemplate().query(sql.toString(), params.toArray(), rs ->
        {
            Long firstFound = null;
            int count = 0;
            while (rs.next() && count++ < offset)
            {
                firstFound = rs.getLong(1);
            }

            return count >= offset ? Optional.ofNullable(firstFound) : Optional.empty();
        });
    }

    private void close(ResultSet rs)
    {
        if (rs == null)
        {
            return;
        }

        try
        {
            rs.close();
        }
        catch (SQLException ignore)
        {

        }
    }

    private void close(Statement statement)
    {
        if (statement == null)
        {
            return;
        }

        try
        {
            statement.close();
        }
        catch (SQLException ignore)
        {

        }
    }

    private void close(Connection connection)
    {
        if (connection == null)
        {
            return;
        }

        try
        {
            connection.close();
        }
        catch (SQLException ignore)
        {

        }
    }

    public int clearExpired()
    {
        final Map<String, Object> params = Collections.singletonMap("ts", getTtlTs());
        return tpl.update("DELETE FROM kfka WHERE timestamp < :ts", params);
    }
}

