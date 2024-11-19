package com.redis.lettucemod;

import java.time.Duration;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.protocol.PushHandler;

/**
 * A thread-safe connection to a RedisStack server. Multiple threads may share
 * one {@link StatefulRedisModulesConnectionImpl}
 * A {@link ConnectionWatchdog} monitors each connection and reconnects
 * automatically until {@link #close} is called. All pending commands will be
 * (re)sent after successful reconnection.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Mark Paluch
 * @author Julien Ruaux
 */
public class StatefulRedisModulesConnectionImpl<K, V> extends StatefulRedisConnectionImpl<K, V> implements StatefulRedisModulesConnection<K, V> {

    /**
     * Initialize a new connection.
     *
     * @param writer      the channel writer.
     * @param pushHandler the handler for push notifications.
     * @param codec       Codec used to encode/decode keys and values.
     * @param timeout     Maximum time to wait for a response.
     */
    public StatefulRedisModulesConnectionImpl(RedisChannelWriter writer, PushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout) {
        super(writer, pushHandler, codec, timeout);
    }

    /**
     * Create a new instance of {@link RedisModulesAsyncCommandsImpl}. Can be
     * overridden to extend.
     */
    @Override
    protected RedisModulesAsyncCommandsImpl<K, V> newRedisAsyncCommandsImpl() {
        return new RedisModulesAsyncCommandsImpl<>(this, codec);
    }

    /**
     * Create a new instance of {@link RedisModulesReactiveCommandsImpl}. Can be
     * overridden to extend.
     */
    @Override
    protected RedisReactiveCommandsImpl<K, V> newRedisReactiveCommandsImpl() {
        return new RedisModulesReactiveCommandsImpl<>(this, codec);
    }

    /**
     * Create a new instance of {@link RedisModulesCommands}. Can be overridden to
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
