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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StringUtils;

import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
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
        final String sql = "SELECT * FROM kfka WHERE timestamp > ? order by id ASC";

        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        try
        {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setLong(1, getTtlTs());
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
        tpl.update(sql, params);
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
        return 0;
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
        tpl.update("TRUNCATE kfka", Collections.emptyMap());
    }

    @Override
    public void sendAfter(final long messageId, final KfkaMessageListener l)
    {

    }

    @Override
    public Optional<Long> getOffsetMessageId(final int offset)
    {
        final String sql = "SELECT * FROM kfka WHERE timestamp > ? order by id DESC LIMIT ?,1";

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try
        {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setLong(1, getTtlTs());
            stmt.setInt(2, offset);
            rs = stmt.executeQuery();
            if (rs.next())
            {
                return Optional.of(rs.getLong("id"));
            }
            return Optional.empty();
        }
        catch (SQLException e)
        {
            throw new DataAccessResourceFailureException(e.getMessage(), e);
        } finally
        {
            close(rs);
            close(stmt);
            close(conn);
        }
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

