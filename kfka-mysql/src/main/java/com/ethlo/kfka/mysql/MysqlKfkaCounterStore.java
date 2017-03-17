package com.ethlo.kfka.mysql;

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

import java.util.Collections;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.ethlo.kfka.persistence.KfkaCounterStore;

public class MysqlKfkaCounterStore implements KfkaCounterStore
{
    private final NamedParameterJdbcTemplate tpl;

    public MysqlKfkaCounterStore(DataSource ds)
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
