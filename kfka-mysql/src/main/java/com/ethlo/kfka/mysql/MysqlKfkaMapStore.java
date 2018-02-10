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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
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

public class MysqlKfkaMapStore<T extends KfkaMessage> implements KfkaMapStore<T>
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlKfkaMapStore.class);
    
    private final IterableNamedParameterJdbcTemplate tpl;
    private final RowMapper<T> mapper;
    private final Duration ttl;
    
    public MysqlKfkaMapStore(DataSource dataSource, RowMapper<T> mapper, Duration ttl)
    {
        this.tpl = new IterableNamedParameterJdbcTemplate(dataSource);
        this.mapper = mapper;
        this.ttl = ttl;
    }
    
    @Override
    public T load(Long key)
    {
        logger.debug("Loading for key {}", key);
        final Map<String, Object> params = new TreeMap<>();
        params.put("key", key);
        params.put("ts", getTtlTs());
        final List<T> res = tpl.query("SELECT * from kfka WHERE id = :key AND timestamp > :ts", params, mapper);
        if (! res.isEmpty())
        {
            return res.get(0);
        }
        return null;
    }

    private long getTtlTs()
    {
        return System.currentTimeMillis() - ttl.toMillis();
    }

    @Override
    public Map<Long, T> loadAll(Collection<Long> keys)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Loading data for keys {}", StringUtils.collectionToCommaDelimitedString(keys));
        }
        
        final Map<String, Object> params = new TreeMap<>();
        params.put("keys", keys);
        params.put("ts", getTtlTs());
        final List<T> res = tpl.query("SELECT * FROM kfka WHERE id IN (:keys) AND timestamp > :ts", params, mapper);
        final Map<Long, T> retVal = new HashMap<>(keys.size());
        res.forEach(e -> retVal.put(e.getId(), e));
        return retVal;
    }

    @Override
    public Iterable<Long> loadAllKeys()
    {
        final Map<String, Object> params = new TreeMap<>();
        params.put("ts", getTtlTs());

        return new CloseableIterable<Long>()
        {
            @Override
            protected CloseableIterator<Long> closeableIterator()
            {
                return tpl.queryForIter("SELECT id FROM kfka WHERE timestamp > :ts", params, new RowMapper<Long>()
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

    @Override
    public int clearExpired()
    {
        final Map<String, Object> params = Collections.singletonMap("ts", getTtlTs());
        return tpl.update("DELETE FROM kfka WHERE timestamp < :ts", params);
    }
}

