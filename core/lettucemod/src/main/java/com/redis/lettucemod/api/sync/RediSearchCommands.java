package com.redis.lettucemod.api.sync;

import java.util.List;

import com.redis.lettucemod.search.*;

public interface RediSearchCommands<K, V> extends io.lettuce.core.api.sync.RediSearchCommands<K, V> {

    List<Object> ftInfo(K index);

}
