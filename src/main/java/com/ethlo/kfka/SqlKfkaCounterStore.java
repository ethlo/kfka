package com.ethlo.kfka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class SqlKfkaCounterStore implements KfkaCounterStore
{
    @Autowired
    public JdbcTemplate tpl;

    @Override
    public long latest()
    {
        final Long latest = tpl.queryForObject("SELECT MAX(id) FROM kfka", Long.class);
        return latest != null ? latest : 0;
    }
}
