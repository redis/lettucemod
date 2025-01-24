package com.redis.lettucemod;

import java.time.Duration;
import java.util.Arrays;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.internal.LettuceAssert;

public class RedisURIBuilder {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = RedisURI.DEFAULT_REDIS_PORT;
	public static final Duration DEFAULT_TIMEOUT_DURATION = RedisURI.DEFAULT_TIMEOUT_DURATION;
	public static final long DEFAULT_TIMEOUT = RedisURI.DEFAULT_TIMEOUT;
	public static final SslVerifyMode DEFAULT_VERIFY_MODE = SslVerifyMode.FULL;

	private RedisURI uri;
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String socket;
	private String username;
	private char[] password;
	private Duration timeout = DEFAULT_TIMEOUT_DURATION;
	private int database;
	private String clientName;
	private String libraryName;
	private String libraryVersion;
	private boolean tls;
	private SslVerifyMode verifyMode = DEFAULT_VERIFY_MODE;

	public RedisURI build() {
		RedisURI.Builder builder = redisURIBuilder();
		if (!RedisModulesUtils.isEmpty(password)) {
			if (RedisModulesUtils.hasLength(username)) {
				builder.withAuthentication(username, password);
			} else {
				builder.withPassword(password);
			}
		}
		if (database > 0) {
			builder.withDatabase(database);
		}
		if (tls) {
			builder.withSsl(tls);
		}
		builder.withVerifyPeer(verifyMode);
		if (timeout != null) {
			builder.withTimeout(timeout);
		}
		RedisURI redisURI = builder.build();
		redisURI.setLibraryName(libraryName);
		redisURI.setLibraryVersion(libraryVersion);
		if (RedisModulesUtils.hasLength(clientName) && !RedisModulesUtils.hasLength(redisURI.getClientName())) {
			redisURI.setClientName(clientName);
		}
		return redisURI;
	}

	private RedisURI.Builder redisURIBuilder() {
		if (uri != null) {
			return RedisURI.builder(uri);
		}
		if (RedisModulesUtils.hasLength(socket)) {
			return RedisURI.Builder.socket(socket);
		}
		return RedisURI.Builder.redis(host, port);
	}

	public RedisURIBuilder uri(RedisURI uri) {
		this.uri = uri;
		return this;
	}

	public RedisURIBuilder host(String host) {
		this.host = host;
		return this;
	}

	public RedisURIBuilder port(int port) {
		this.port = port;
		return this;
	}

	public RedisURIBuilder socket(String socket) {
		this.socket = socket;
		return this;
	}

	public RedisURIBuilder username(String username) {
		this.username = username;
		return this;
	}

	public RedisURIBuilder password(String password) {
		LettuceAssert.notNull(password, "Password must not be null");
		return password(password.toCharArray());
	}

	public RedisURIBuilder password(char[] password) {
		this.password = password;
		return this;
	}

	public RedisURIBuilder timeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	public RedisURIBuilder database(int database) {
		this.database = database;
		return this;
	}

	public RedisURIBuilder clientName(String clientName) {
		this.clientName = clientName;
		return this;
	}

	public RedisURIBuilder tls(boolean tls) {
		this.tls = tls;
		return this;
	}

	public RedisURIBuilder verifyMode(SslVerifyMode verifyMode) {
		this.verifyMode = verifyMode;
		return this;
	}

	@Override
	public String toString() {
		return "RedisURIBuilder [uri=" + uri + ", host=" + host + ", port=" + port + ", socket=" + socket
				+ ", username=" + username + ", password=" + Arrays.toString(password) + ", timeout=" + timeout
				+ ", database=" + database + ", clientName=" + clientName + ", tls=" + tls + ", verifyMode="
				+ verifyMode + "]";
	}

}
