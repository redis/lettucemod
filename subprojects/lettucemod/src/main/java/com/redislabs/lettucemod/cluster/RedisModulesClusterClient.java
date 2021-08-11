package com.redislabs.lettucemod.cluster;

import com.redislabs.lettucemod.StatefulRedisModulesConnectionImpl;
import com.redislabs.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.*;
import io.lettuce.core.cluster.*;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.PushHandler;
import io.lettuce.core.resource.ClientResources;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;

public class RedisModulesClusterClient extends RedisClusterClient {

    protected RedisModulesClusterClient(ClientResources clientResources, Iterable<RedisURI> redisURIs) {
        super(clientResources, redisURIs);
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with default {@link ClientResources}. You can
     * connect to different Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param redisURI the Redis URI, must not be {@code null}
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisModulesClusterClient create(RedisURI redisURI) {
        assertNotNull(redisURI);
        return create(Collections.singleton(redisURI));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with default {@link ClientResources}. You can
     * connect to different Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param redisURIs one or more Redis URI, must not be {@code null} and not empty.
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisModulesClusterClient create(Iterable<RedisURI> redisURIs) {
        assertNotEmpty(redisURIs);
        assertSameOptions(redisURIs);
        return new RedisModulesClusterClient(null, redisURIs);
    }

    private static void assertNotEmpty(Iterable<RedisURI> redisURIs) {
        LettuceAssert.notNull(redisURIs, "RedisURIs must not be null");
        LettuceAssert.isTrue(redisURIs.iterator().hasNext(), "RedisURIs must not be empty");
    }

    private static void assertSameOptions(Iterable<RedisURI> redisURIs) {

        Boolean ssl = null;
        Boolean startTls = null;
        Boolean verifyPeer = null;

        for (RedisURI redisURI : redisURIs) {

            if (ssl == null) {
                ssl = redisURI.isSsl();
            }
            if (startTls == null) {
                startTls = redisURI.isStartTls();
            }
            if (verifyPeer == null) {
                verifyPeer = redisURI.isVerifyPeer();
            }

            if (ssl != redisURI.isSsl()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " SSL is not consistent with the other seed URI SSL settings");
            }

            if (startTls != redisURI.isStartTls()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " StartTLS is not consistent with the other seed URI StartTLS settings");
            }

            if (verifyPeer != redisURI.isVerifyPeer()) {
                throw new IllegalArgumentException(
                        "RedisURI " + redisURI + " VerifyPeer is not consistent with the other seed URI VerifyPeer settings");
            }
        }
    }

    /**
     * Create a new client that connects to the supplied uri with default {@link ClientResources}. You can connect to different
     * Redis servers but you must supply a {@link RedisURI} on connecting.
     *
     * @param uri the Redis URI, must not be empty or {@code null}.
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisModulesClusterClient create(String uri) {
        LettuceAssert.notEmpty(uri, "URI must not be empty");
        return create(RedisClusterURIUtil.toRedisURIs(URI.create(uri)));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with shared {@link ClientResources}. You need to
     * shut down the {@link ClientResources} upon shutting down your application.You can connect to different Redis servers but
     * you must supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}
     * @param redisURI        the Redis URI, must not be {@code null}
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisModulesClusterClient create(ClientResources clientResources, RedisURI redisURI) {
        assertNotNull(clientResources);
        assertNotNull(redisURI);
        return create(clientResources, Collections.singleton(redisURI));
    }

    /**
     * Create a new client that connects to the supplied uri with shared {@link ClientResources}.You need to shut down the
     * {@link ClientResources} upon shutting down your application. You can connect to different Redis servers but you must
     * supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}
     * @param uri             the Redis URI, must not be empty or {@code null}.
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisClusterClient create(ClientResources clientResources, String uri) {
        assertNotNull(clientResources);
        LettuceAssert.notEmpty(uri, "URI must not be empty");
        return create(clientResources, RedisClusterURIUtil.toRedisURIs(URI.create(uri)));
    }

    /**
     * Create a new client that connects to the supplied {@link RedisURI uri} with shared {@link ClientResources}. You need to
     * shut down the {@link ClientResources} upon shutting down your application.You can connect to different Redis servers but
     * you must supply a {@link RedisURI} on connecting.
     *
     * @param clientResources the client resources, must not be {@code null}
     * @param redisURIs       one or more Redis URI, must not be {@code null} and not empty
     * @return a new instance of {@link RedisClusterClient}
     */
    public static RedisModulesClusterClient create(ClientResources clientResources, Iterable<RedisURI> redisURIs) {
        assertNotNull(clientResources);
        assertNotEmpty(redisURIs);
        assertSameOptions(redisURIs);
        return new RedisModulesClusterClient(clientResources, redisURIs);
    }

    private static void assertNotNull(RedisURI redisURI) {
        LettuceAssert.notNull(redisURI, "RedisURI must not be null");
    }

    private static void assertNotNull(ClientResources clientResources) {
        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
    }

    @Override
    public StatefulRedisModulesClusterConnection<String, String> connect() {
        return connect(newStringStringCodec());
    }

    @Override
    public <K, V> StatefulRedisModulesClusterConnection<K, V> connect(RedisCodec<K, V> codec) {
        return (StatefulRedisModulesClusterConnection<K, V>) super.connect(codec);
    }

    @Override
    protected <K, V> StatefulRedisModulesConnectionImpl<K, V> newStatefulRedisConnection(RedisChannelWriter channelWriter, PushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout) {
        return new StatefulRedisModulesConnectionImpl<>(channelWriter, pushHandler, codec, timeout);
    }

    @Override
    protected <V, K> StatefulRedisModulesClusterConnectionImpl<K, V> newStatefulRedisClusterConnection(RedisChannelWriter channelWriter, ClusterPushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout) {
        return new StatefulRedisModulesClusterConnectionImpl<>(channelWriter, pushHandler, codec, timeout);
    }

}
