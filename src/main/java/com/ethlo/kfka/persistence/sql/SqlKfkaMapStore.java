package com.ethlo.kfka.persistence.sql;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import com.alexkasko.springjdbc.iterable.CloseableIterable;
import com.alexkasko.springjdbc.iterable.CloseableIterator;
import com.alexkasko.springjdbc.iterable.IterableNamedParameterJdbcTemplate;
import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.persistence.KfkaMapStore;

public class SqlKfkaMapStore<T extends KfkaMessage> implements KfkaMapStore<T>
{
    private final static Logger logger = LoggerFactory.getLogger(SqlKfkaMapStore.class);
    
    private final IterableNamedParameterJdbcTemplate tpl;
    private RowMapper<T> mapper;
    
    public SqlKfkaMapStore(DataSource dataSource, RowMapper<T> mapper)
    {
        this.tpl = new IterableNamedParameterJdbcTemplate(dataSource);
        this.mapper = mapper;
    }
    
    @Override
    public T load(Long key)
    {
        logger.debug("Loading for key {}", key);
        final List<T> res = tpl.query("SELECT * from kfka WHERE id = :key", Collections.singletonMap("key", key), mapper);
        if (! res.isEmpty())
        {
            return res.get(0);
        }
        return null;
    }

    @Override
    public Map<Long, T> loadAll(Collection<Long> keys)
    {
        logger.debug("Loading data for keys {}", StringUtils.collectionToCommaDelimitedString(keys));
        final List<T> res = tpl.query("SELECT * FROM kfka WHERE id IN (:keys)", Collections.singletonMap("keys", keys), mapper);
        final Map<Long, T> retVal = new HashMap<>(keys.size());
        res.forEach(e -> {retVal.put(e.getId(), e);});
        return retVal;
    }

    @Override
    public Iterable<Long> loadAllKeys()
    {
        return new CloseableIterable<Long>()
        {
            @Override
            protected CloseableIterator<Long> closeableIterator()
            {
                return tpl.queryForIter("SELECT id FROM kfka", Collections.emptyMap(), new RowMapper<Long>()
                {
                    @Override
                    public Long mapRow(ResultSet rs, int rowNum) throws SQLException
                    {
                        return rs.getLong("id");
                    }
                });
            }
        };
    }

    @Override
    public void store(Long key, KfkaMessage value)
    {
        final Map<String, ?> params = getInsertParams(value);
        final String sql = getInsertSql(value);
        tpl.update(sql, params);
    }

    private String getInsertSql(KfkaMessage value)
    {
        final Collection<String> extraProps = value.getQueryableProperties();
        final String extraColsStr = (extraProps.isEmpty() ? "" : (", " + StringUtils.collectionToCommaDelimitedString(extraProps)));
        final String extraColPlaceholdersStr = (extraProps.isEmpty() ? "" : (", :" + StringUtils.collectionToDelimitedString(extraProps, ", :")));
        return "INSERT INTO kfka (id, topic, type, timestamp, payload" + extraColsStr + ")"
                        + " VALUES(:id, :topic, :type, :timestamp, :payload" + extraColPlaceholdersStr + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void storeAll(Map<Long, T> map)
    {
        if (map.isEmpty())
        {
            return;
        }
        
        final KfkaMessage first = map.values().iterator().next();
        final String sql = getInsertSql(first);
        final List<Map<String, ?>> parameters = new LinkedList<>();
        for (Entry<Long, T> entry : map.entrySet())
        {
            parameters.add(getInsertParams(entry.getValue()));
        }
        tpl.batchUpdate(sql, (Map<String, ?>[]) parameters.toArray(new Map[parameters.size()]));
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
            if (! retVal.containsKey(propName))
            {
                retVal.put(propName, KfkaMessage.getPropertyValue(value, propName));
            }
        }
        
        return retVal;
    }

    @Override
    public void delete(Long key)
    {
        tpl.update("DELETE FROM kfka WHERE id = :key", Collections.singletonMap("key", key));
    }

    @Override
    public void deleteAll(Collection<Long> keys)
    {
        tpl.update("DELETE FROM kfka WHERE id IN (:keys)", Collections.singletonMap("keys", keys));        
    }
}

