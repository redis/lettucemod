package com.redislabs.mesclun.gears;

import io.lettuce.core.RedisFuture;

import java.util.List;

public interface RedisGearsAsyncCommands<K, V> {

    RedisFuture<String> pyExecute(String function, PyExecuteOptions options);

    RedisFuture<List<Registration>> dumpRegistrations();

}
