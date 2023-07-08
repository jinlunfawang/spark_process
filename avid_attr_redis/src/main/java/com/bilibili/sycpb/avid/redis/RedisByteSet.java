package com.bilibili.sycpb.avid.redis;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.Serializable;

/**
 * @file: RedisByteSet
 * @author: chenzhe01@bilibili.com
 * @date: 2021/6/3 10:59
 * @version: 1.0
 * @description: 写描述
 */
public class RedisByteSet implements Serializable {
    private final byte[] key;
    private final byte[] value;
    private final int expire;
    private Response<String> response;

    public RedisByteSet(byte[] key, byte[] value, int expire) {
        this.key = key;
        this.value = value;
        this.expire = expire;
    }

    public RedisByteSet(byte[] key, byte[] value) {
        this(key, value, -1);
    }

    public void execute(Pipeline pipeline) {
        if (expire > 0) {
            response = pipeline.setex(key, expire, value);
        } else {
            response = pipeline.set(key, value);
        }
    }

    public String getKey() {
        return new String(key);
    }

    public long getHeapSize() {
        return key.length + value.length;
    }

    public Response<String> getResponse() { return response; }
}
