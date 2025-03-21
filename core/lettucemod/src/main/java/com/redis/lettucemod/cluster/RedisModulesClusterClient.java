package com.redis.lettucemod.cluster;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Supplier;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.StatefulRedisModulesConnectionImpl;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterPushHandler;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.RedisClusterURIUtil;
import io.lettuce.core.cluster.StatefulRedisClusterConnectionImpl;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.json.JsonParser;
import io.lettuce.core.protocol.PushHandler;
import io.lettuce.core.resource.ClientResources;

public class RedisModulesClusterClient extends RedisClusterClient {

	public static final ClusterClientOptions DEFAULT_CLIENT_OPTIONS = RedisModulesClient
			.defaultClientOptions(ClusterClientOptions.builder()).build();

	protected RedisModulesClusterClient(ClientResources clientResources, Iterable<RedisURI> redisURIs) {
		super(clientResources, redisURIs);
		setOptions(DEFAULT_CLIENT_OPTIONS);
	}

	/**
	 * Create a new client that connects to the supplied {@link RedisURI uri} with
	 * default {@link ClientResources}. You can connect to different Redis servers
	 * but you must supply a {@link RedisURI} on connecting.
	 *
	 * @param redisURI the Redis URI, must not be {@code null}
	 * @return a new instance of {@link RedisClusterClient}
	 */
	public static RedisModulesClusterClient create(RedisURI redisURI) {
		assertNotNull(redisURI);
		return create(Collections.singleton(redisURI));
	}

	/**
	 * Create a new client that connects to the supplied {@link RedisURI uri} with
	 * default {@link ClientResources}. You can connect to different Redis servers
	 * but you must supply a {@link RedisURI} on connecting.
	 *
	 * @param redisURIs one or more Redis URI, must not be {@code null} and not
	 *                  empty.
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
				fail("SSL is not consistent with the other seed URI SSL settings", redisURI);
			}

			if (startTls != redisURI.isStartTls()) {
				fail("StartTLS is not consistent with the other seed URI StartTLS settings", redisURI);
			}

			if (verifyPeer != redisURI.isVerifyPeer()) {
				fail("VerifyPeer is not consistent with the other seed URI VerifyPeer settings", redisURI);
			}
		}
	}

	private static void fail(String message, RedisURI redisURI) {
		throw new IllegalArgumentException("RedisURI " + redisURI + " " + message);
	}

	/**
	 * Create a new client that connects to the supplied uri with default
	 * {@link ClientResources}. You can connect to different Redis servers but you
	 * must supply a {@link RedisURI} on connecting.
	 *
	 * @param uri the Redis URI, must not be empty or {@code null}.
	 * @return a new instance of {@link RedisClusterClient}
	 */
	public static RedisModulesClusterClient create(String uri) {
		LettuceAssert.notEmpty(uri, "URI must not be empty");
		return create(RedisClusterURIUtil.toRedisURIs(URI.create(uri)));
	}

	/**
	 * Create a new client that connects to the supplied {@link RedisURI uri} with
	 * shared {@link ClientResources}. You need to shut down the
	 * {@link ClientResources} upon shutting down your application.You can connect
	 * to different Redis servers but you must supply a {@link RedisURI} on
	 * connecting.
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
	 * Create a new client that connects to the supplied uri with shared
	 * {@link ClientResources}.You need to shut down the {@link ClientResources}
	 * upon shutting down your application. You can connect to different Redis
	 * servers but you must supply a {@link RedisURI} on connecting.
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
	 * Create a new client that connects to the supplied {@link RedisURI uri} with
	 * shared {@link ClientResources}. You need to shut down the
	 * {@link ClientResources} upon shutting down your application.You can connect
	 * to different Redis servers but you must supply a {@link RedisURI} on
	 * connecting.
	 *
	 * @param clientResources the client resources, must not be {@code null}
	 * @param redisURIs       one or more Redis URI, must not be {@code null} and
	 *                        not empty
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
		StatefulRedisClusterConnection<K, V> connection = super.connect(codec);
		return (StatefulRedisModulesClusterConnection<K, V>) connection;
	}

	@Override
	protected <K, V> StatefulRedisModulesConnectionImpl<K, V> newStatefulRedisConnection(
			RedisChannelWriter channelWriter, PushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout) {
		return new StatefulRedisModulesConnectionImpl<>(channelWriter, pushHandler, codec, timeout);
	}

	@Override
	protected <K, V> StatefulRedisConnectionImpl<K, V> newStatefulRedisConnection(RedisChannelWriter channelWriter,
			PushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout, Supplier<JsonParser> parser) {
		return new StatefulRedisModulesConnectionImpl<>(channelWriter, pushHandler, codec, timeout, parser);
	}

	@Override
	protected <V, K> StatefulRedisModulesClusterConnectionImpl<K, V> newStatefulRedisClusterConnection(
			RedisChannelWriter channelWriter, ClusterPushHandler pushHandler, RedisCodec<K, V> codec,
			Duration timeout) {
		return new StatefulRedisModulesClusterConnectionImpl<>(channelWriter, pushHandler, codec, timeout);
	}

	@Override
	protected <V, K> StatefulRedisClusterConnectionImpl<K, V> newStatefulRedisClusterConnection(
			RedisChannelWriter channelWriter, ClusterPushHandler pushHandler, RedisCodec<K, V> codec, Duration timeout,
			Supplier<JsonParser> parser) {
		return new StatefulRedisModulesClusterConnectionImpl<>(channelWriter, pushHandler, codec, timeout, parser);
	}

}
