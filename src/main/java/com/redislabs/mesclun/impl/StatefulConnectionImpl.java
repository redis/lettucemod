/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.mesclun.impl;

import java.time.Duration;

import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.RedisModulesAsyncCommands;
import com.redislabs.mesclun.RedisModulesReactiveCommands;
import com.redislabs.mesclun.RedisModulesCommands;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.protocol.PushHandler;

/**
 * A thread-safe connection to a RedisTimeSeries server. Multiple threads may share
 * one {@link StatefulConnectionImpl}
 * A {@link ConnectionWatchdog} monitors each connection and reconnects
 * automatically until {@link #close} is called. All pending commands will be
 * (re)sent after successful reconnection.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Mark Paluch
 * @author Julien Ruaux
 */
public class StatefulConnectionImpl<K, V> extends StatefulRedisConnectionImpl<K, V> implements StatefulRedisModulesConnection<K, V> {

    /**
     * Initialize a new connection.
     *
     * @param writer      the channel writer.
     * @param pushHandler the handler for push notifications.
     * @param codec       Codec used to encode/decode keys and values.
     * @param timeout     Maximum time to wait for a response.
     */
    public StatefulConnectionImpl(RedisChannelWriter writer, PushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout) {
        super(writer, pushHandler, codec, timeout);
    }

    /**
     * Create a new instance of {@link AsyncCommandsImpl}. Can be
     * overriden to extend.
     */
    @Override
    protected AsyncCommandsImpl<K, V> newRedisAsyncCommandsImpl() {
        return new AsyncCommandsImpl<>(this, codec);
    }

    /**
     * Create a new instance of {@link ReactiveCommandsImpl}. Can be
     * overriden to extend.
     */
    @Override
    protected RedisReactiveCommandsImpl<K, V> newRedisReactiveCommandsImpl() {
        return new ReactiveCommandsImpl<>(this, codec);
    }

    /**
     * Create a new instance of {@link RedisModulesCommands}. Can be overriden to
     * extend.
     *
     * @return a new instance
     */
    @Override
    protected RedisModulesCommands<K, V> newRedisSyncCommandsImpl() {
        return syncHandler(async(), RedisModulesCommands.class, RedisClusterCommands.class);
    }

    @Override
    public RedisModulesAsyncCommands<K, V> async() {
        return (RedisModulesAsyncCommands<K, V>) super.async();
    }

    @Override
    public RedisModulesCommands<K, V> sync() {
        return (RedisModulesCommands<K, V>) super.sync();
    }

    @Override
    public RedisModulesReactiveCommands<K, V> reactive() {
        return (RedisModulesReactiveCommands<K, V>) super.reactive();
    }

}
