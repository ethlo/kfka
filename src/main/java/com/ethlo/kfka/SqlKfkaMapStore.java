package com.ethlo.kfka;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import com.alexkasko.springjdbc.iterable.CloseableIterable;
import com.alexkasko.springjdbc.iterable.CloseableIterator;
import com.alexkasko.springjdbc.iterable.IterableNamedParameterJdbcTemplate;
import com.hazelcast.core.MapStore;

@Service
public class SqlKfkaMapStore implements MapStore<Long, KfkaMessage>
{
    private static final RowMapper<KfkaMessage> ROW_MAPPER = new RowMapper<KfkaMessage>()
    {
        @Override
        public KfkaMessage mapRow(ResultSet rs, int rowNum) throws SQLException
        {
            return new KfkaMessage.Builder()
                 .message(rs.getString("message"))
                 .organzationId(rs.getLong("organization_id"))
                 .userId(rs.getLong("user_id"))
                 .timestamp(rs.getLong("timestamp"))
                 .topic(rs.getString("topic"))
                 .type(rs.getString("type"))
                 .build().id(rs.getLong("id"));
        }
    };

    private static final String INSERT_SQL = "INSERT INTO kfka (id, topic, type, timestamp, message, organization_id, user_id) "
          + "VALUES(:id, :topic, :type, :timestamp, :message, :organization_id, :user_id)";
    
    private IterableNamedParameterJdbcTemplate tpl;
    
    @Autowired
    public SqlKfkaMapStore(DataSource dataSource)
    {
        this.tpl = new IterableNamedParameterJdbcTemplate(dataSource);
    }
    
    
    @Override
    public KfkaMessage load(Long key)
    {
        final List<KfkaMessage> res = tpl.query("SELECT * from kfka WHERE id = :key", Collections.singletonMap("key", key), ROW_MAPPER);
        if (! res.isEmpty())
        {
            return res.get(0);
        }
        return null;
    }

    @Override
    public Map<Long, KfkaMessage> loadAll(Collection<Long> keys)
    {
        final List<KfkaMessage> res = tpl.query("SELECT * FROM kfka WHERE id IN (:keys)", Collections.singletonMap("keys", keys), ROW_MAPPER);
        final Map<Long, KfkaMessage> retVal = new HashMap<>(keys.size());
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
        tpl.update(INSERT_SQL, params);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void storeAll(Map<Long, KfkaMessage> map)
    {
        final List<Map<String, ?>> parameters = new LinkedList<>();
        for (Entry<Long, KfkaMessage> entry : map.entrySet())
        {
            parameters.add(getInsertParams(entry.getValue()));
        }
        tpl.batchUpdate(INSERT_SQL, (Map<String, ?>[]) parameters.toArray(new Map[parameters.size()]));
    }

    private Map<String, Object> getInsertParams(KfkaMessage value)
    {
        final Map<String, Object> retVal = new TreeMap<>();
        retVal.put("id", value.getId());
        retVal.put("message", value.getMessage());
        retVal.put("type", value.getType());
        retVal.put("topic", value.getTopic());
        retVal.put("user_id", value.getUserId());
        retVal.put("organization_id", value.getOrganzationId());
        retVal.put("timestamp", new Date(value.getTimestamp()));
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

