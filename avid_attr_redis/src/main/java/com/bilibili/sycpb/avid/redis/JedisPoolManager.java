package com.bilibili.sycpb.avid.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * @file: JedisPoolManager
 * @author: chenzhe01@bilibili.com
 * @date: 2021/6/3 11:01
 * @version: 1.0
 * @description: 写描述
 */
public class JedisPoolManager  implements Serializable {

    private static final Log LOG = LogFactory.getLog(JedisPoolManager.class);

    private static final Field FIELD_CONNECTION_HANDLER;
    private static final Field FIELD_CACHE;

    static {
        FIELD_CONNECTION_HANDLER = getField(BinaryJedisCluster.class, "connectionHandler");
        FIELD_CACHE = getField(JedisClusterConnectionHandler.class, "cache");
    }

    private JedisCluster client;

    public JedisPoolManager(JedisCluster client) {
        this.client = client;
    }

    public JedisPool getJedisPool(String key) {
        try {
            JedisSlotBasedConnectionHandler connectionHandler = getValue(client, FIELD_CONNECTION_HANDLER);
            JedisClusterInfoCache clusterInfoCache = getValue(connectionHandler, FIELD_CACHE);
            int slot = JedisClusterCRC16.getSlot(key);
            return clusterInfoCache.getSlotPool(slot);
        } catch (Exception e) {
            LOG.warn("Get Jedis pool failure!", e);
        }
        return null;
    }

    public void refreshCluster() {
        try {
            JedisSlotBasedConnectionHandler connectionHandler = getValue(client, FIELD_CONNECTION_HANDLER);
            connectionHandler.renewSlotCache();
        } catch (Exception e) {
            LOG.warn("refreshCluster fail!", e);
        }
    }

    private static Field getField(Class<?> cls, String fieldName) {
        try {
            Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException("Can not find or access field '" + fieldName + "' from " + cls.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getValue(Object obj, Field field) throws IllegalAccessException {
        return (T)field.get(obj);
    }
}

