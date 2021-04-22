package com.ethlo.kfka.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import javax.sql.DataSource;

public class SimpleJdbcTemplate
{
    private final DataSource dataSource;

    public SimpleJdbcTemplate(DataSource dataSource)
    {
        this.dataSource = dataSource;
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

    public <T> T query(String sql, List<Object> params, Function<ResultSet, T> callback)
    {
        try (final Connection conn = dataSource.getConnection(); final PreparedStatement stat = setParams(conn.prepareStatement(sql), params))
        {
            final List<T> result = new LinkedList<>();
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

    public <T> T queryForObject(String sql, List<Object> params, Class<T> type)
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
            return null;
        }

        if (result.size() > 1)
        {
            throw new RuntimeSqlException(new SQLException("More than one result found for query"));
        }

        return result.get(0);
    }

    public long update(final String sql, final List<Object> params)
    {
        try (final Connection conn = dataSource.getConnection(); final PreparedStatement stat = conn.prepareStatement(sql))
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
}