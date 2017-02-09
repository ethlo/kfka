package com.ethlo.kfka.persistence.sql;

import java.util.Collections;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.ethlo.kfka.persistence.KfkaCounterStore;

public class SqlKfkaCounterStore implements KfkaCounterStore
{
    private final NamedParameterJdbcTemplate tpl;

    public SqlKfkaCounterStore(DataSource ds)
    {
        this.tpl = new NamedParameterJdbcTemplate(ds);
    }
    
    @Override
    public long latest()
    {
        final Long latest = tpl.queryForObject("SELECT MAX(id) FROM kfka", Collections.emptyMap(), Long.class);
        return latest != null ? latest : 0;
    }
}
