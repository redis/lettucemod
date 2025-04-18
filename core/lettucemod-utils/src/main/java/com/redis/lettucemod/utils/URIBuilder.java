package com.redis.lettucemod.utils;

import java.lang.reflect.Array;
import java.time.Duration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import lombok.ToString;

@ToString
public class URIBuilder {

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
        Builder builder = redisURIBuilder();
        if (password != null && Array.getLength(password) > 0) {
            if (LettuceStrings.isNotEmpty(username)) {
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
        if (LettuceStrings.isNotEmpty(libraryName) && LettuceStrings.isEmpty(redisURI.getLibraryName())) {
            redisURI.setLibraryName(libraryName);
        }
        if (LettuceStrings.isNotEmpty(libraryVersion) && LettuceStrings.isEmpty(redisURI.getLibraryVersion())) {
            redisURI.setLibraryVersion(libraryVersion);
        }
        if (LettuceStrings.isNotEmpty(clientName) && LettuceStrings.isEmpty(redisURI.getClientName())) {
            redisURI.setClientName(clientName);
        }
        return redisURI;
    }

    private Builder redisURIBuilder() {
        if (uri != null) {
            Builder builder = RedisURI.builder(uri);
            if (LettuceStrings.isNotEmpty(uri.getSentinelMasterId())) {
                builder.withSentinelMasterId(uri.getSentinelMasterId());
            }
            uri.getSentinels().forEach(builder::withSentinel);
            return builder;
        }
        if (LettuceStrings.isNotEmpty(socket)) {
            return Builder.socket(socket);
        }
        return Builder.redis(host, port);
    }

    public URIBuilder uri(String uri) {
        return uri(RedisURI.create(uri));
    }

    public URIBuilder uri(RedisURI uri) {
        this.uri = uri;
        return this;
    }

    public URIBuilder host(String host) {
        this.host = host;
        return this;
    }

    public URIBuilder port(int port) {
        this.port = port;
        return this;
    }

    public URIBuilder socket(String socket) {
        this.socket = socket;
        return this;
    }

    public URIBuilder username(String username) {
        this.username = username;
        return this;
    }

    public URIBuilder password(String password) {
        LettuceAssert.notNull(password, "Password must not be null");
        return password(password.toCharArray());
    }

    public URIBuilder password(char[] password) {
        this.password = password;
        return this;
    }

    public URIBuilder timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public URIBuilder database(int database) {
        this.database = database;
        return this;
    }

    public URIBuilder clientName(String clientName) {
        this.clientName = clientName;
        return this;
    }

    public URIBuilder libraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public URIBuilder libraryVersion(String libraryVersion) {
        this.libraryVersion = libraryVersion;
        return this;
    }

    public URIBuilder tls(boolean tls) {
        this.tls = tls;
        return this;
    }

    public URIBuilder noVerifyPeer() {
        return verifyPeer(false);
    }

    public URIBuilder verifyPeer(boolean enable) {
        return verifyMode(enable ? SslVerifyMode.FULL : SslVerifyMode.NONE);
    }

    public URIBuilder verifyMode(SslVerifyMode verifyMode) {
        this.verifyMode = verifyMode;
        return this;
    }

    public static URIBuilder of(String uri) {
        return new URIBuilder().uri(uri);
    }

    public static URIBuilder of(RedisURI uri) {
        return new URIBuilder().uri(uri);
    }

    public static URIBuilder of(String host, int port) {
        return new URIBuilder().host(host).port(port);
    }

}
