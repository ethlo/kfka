package com.ethlo.kfka.jdbc;

/*-
 * #%L
 * kfka-jdbc
 * %%
 * Copyright (C) 2017 - 2021 Morten Haraldsen (ethlo)
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

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.sql.DataSource;

import com.ethlo.kfka.Assert;

public class SimpleJdbcTemplate
{
    private final DataSource dataSource;

    public SimpleJdbcTemplate(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    private static <K extends Number, T> int doInsertBatch(Connection connection, String sql, Iterator<T> parameters, BiConsumer<T, ParameterSink> parameterSetter, final int batchSize, final BiConsumer<T, K> identityCallback, final Class<K> keyType)
    {
        int total = 0;

        try (final PreparedStatement pst = connection.prepareStatement(sql, identityCallback != null ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS))
        {
            final List<T> buffer = new ArrayList<>();
            while (parameters.hasNext())
            {
                final T params = parameters.next();
                parameterSetter.accept(params, new ParameterSink(pst));
                buffer.add(params);
                pst.addBatch();

                if (buffer.size() == batchSize)
                {
                    total = handleBatch(identityCallback, keyType, pst, buffer);
                }
            }

            total += handleBatch(identityCallback, keyType, pst, buffer);
        }
        catch (SQLException e)
        {
            throw new RuntimeSqlException(e);
        }
        return total;
    }

    private static <K extends Number, T> int handleBatch(final BiConsumer<T, K> identityCallback, final Class<K> keyType, final PreparedStatement pst, final List<T> buffer) throws SQLException
    {
        final int size = buffer.size();
        if (size != 0)
        {
            pst.executeBatch();
            handleKeys(buffer, pst, identityCallback, keyType);
            buffer.clear();
        }
        return size;
    }

    private static <K extends Number, T> void handleKeys(final List<T> buffer, PreparedStatement pst, final BiConsumer<T, K> identityCallback, Class<K> keyType) throws SQLException
    {
        if (identityCallback != null)
        {
            final ResultSet generatedKeys = pst.getGeneratedKeys();
            int i = 0;
            while (generatedKeys.next())
            {
                identityCallback.accept(buffer.get(i++), generatedKeys.getObject(1, keyType));
            }

            final int total = i;
            Assert.isTrue(i == buffer.size(), () -> "Expected " + total + " to match buffer insert size of " + buffer.size());
        }
    }

    public <T> int insertBatch(String sql, Iterator<T> parameters, BiConsumer<T, ParameterSink> parameterSetter, final int batchSize)
    {
        try (final Connection conn = dataSource.getConnection())
        {
            return doInsertBatch(conn, sql, parameters, parameterSetter, batchSize, null, null);
        }
        catch (SQLException e)
        {
            throw new RuntimeSqlException(e);
        }
    }

    public <K extends Number, T> int insertBatch(String sql, Iterator<T> parameters, BiConsumer<T, ParameterSink> parameterSetter, final int batchSize, final BiConsumer<T, K> identityCallback, Class<K> keyType)
    {
        try (final Connection conn = dataSource.getConnection())
        {
            return doInsertBatch(conn, sql, parameters, parameterSetter, batchSize, identityCallback, keyType);
        }
        catch (SQLException e)
        {
            throw new RuntimeSqlException(e);
        }
    }

    public <T> List<T> queryByRow(String sql, List<Object> params, RowMapper<T> mapper)
    {
        return query(sql, params, rs ->
        {
            final List<T> result = new LinkedList<>();
            try
            {
                while (rs.next())
                {
                    result.add(mapper.mapRow(rs));
                }
            }
            catch (SQLException exc)
            {
                throw new RuntimeSqlException(exc);
            }

            return result;
        });
    }

    public <T> T query(final String sql, List<Object> params, Function<ResultSet, T> callback)
    {
        try (final Connection conn = dataSource.getConnection();
             final PreparedStatement stat = setParams(conn.prepareStatement(sql,
                     TYPE_FORWARD_ONLY,
                     CONCUR_READ_ONLY
             ), params))
        {
            stat.setFetchSize(1);
            try (final ResultSet rs = stat.executeQuery())
            {
                return callback.apply(rs);
            }
        }
        catch (SQLException exc)
        {
            throw new RuntimeSqlException(exc);
        }
    }

    public <T> Optional<T> queryForObject(String sql, List<Object> params, Class<T> type)
    {
        final List<T> result = queryByRow(sql, params, rs ->
        {
            try
            {
                return type.cast(rs.getObject(1));
            }
            catch (SQLException exc)
            {
                throw new RuntimeSqlException(exc);
            }
        });

        if (result.isEmpty())
        {
            return Optional.empty();
        }

        if (result.size() > 1)
        {
            throw new RuntimeSqlException(new SQLException("More than one result found for query"));
        }

        return Optional.ofNullable(result.get(0));
    }

    public long update(final String sql, final List<Object> params)
    {
        try (final Connection conn = dataSource.getConnection();
             final PreparedStatement stat = conn.prepareStatement(sql))
        {
            return setParams(stat, params).executeUpdate();
        }
        catch (SQLException exc)
        {
            throw new RuntimeSqlException(exc);
        }
    }

    private PreparedStatement setParams(PreparedStatement stat, List<Object> params)
    {
        for (int i = 0; i < params.size(); i++)
        {
            try
            {
                stat.setObject(i + 1, params.get(i));
            }
            catch (SQLException exc)
            {
                throw new RuntimeSqlException(exc);
            }
        }
        return stat;
    }

    public long insert(final String sql, final List<Object> params)
    {
        try (final Connection conn = dataSource.getConnection(); final PreparedStatement stat = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS))
        {
            final int updated = setParams(stat, params).executeUpdate();
            if (updated != 1)
            {
                throw new RuntimeSqlException(new SQLException("Insert affected " + updated + " rows"));
            }
            final ResultSet generatedKeysRs = stat.getGeneratedKeys();
            if (generatedKeysRs.next())
            {
                return generatedKeysRs.getLong(1);
            }
            throw new RuntimeSqlException(new SQLException("No auto generated key found in JDBC result set"));
        }
        catch (SQLException exc)
        {
            throw new RuntimeSqlException(exc);
        }
    }

    public static class ParameterSink
    {
        private final PreparedStatement pst;

        private ParameterSink(final PreparedStatement pst)
        {
            this.pst = pst;
        }

        public void setObject(int index, Object object)
        {
            try
            {
                pst.setObject(index, object);
            }
            catch (SQLException e)
            {
                throw new RuntimeSqlException(e);
            }
        }
    }
}
