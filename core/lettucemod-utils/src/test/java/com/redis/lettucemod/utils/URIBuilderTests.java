package com.redis.lettucemod.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;

public class URIBuilderTests {

    private static final String TEST_URI_STRING = "redis://localhost:12345";

    private static final RedisURI TEST_URI = RedisURI.create(TEST_URI_STRING);

    @Test
    void testDefaults() {
        URIBuilder builder = new URIBuilder();
        RedisURI uri = builder.build();
        Assertions.assertEquals(URIBuilder.DEFAULT_HOST, uri.getHost());
        Assertions.assertEquals(URIBuilder.DEFAULT_PORT, uri.getPort());
        Assertions.assertEquals(URIBuilder.DEFAULT_TIMEOUT_DURATION, uri.getTimeout());
        Assertions.assertEquals(URIBuilder.DEFAULT_VERIFY_MODE, uri.getVerifyMode());
    }

    @Test
    void testUri() {
        Assertions.assertEquals(TEST_URI, URIBuilder.of(TEST_URI).build());
        Assertions.assertEquals(TEST_URI, URIBuilder.of(TEST_URI_STRING).build());
    }

    @Test
    void testTls() {
        Assertions.assertTrue(URIBuilder.of(TEST_URI).tls(true).build().isSsl());
        Assertions.assertTrue(URIBuilder.of(RedisURI.builder(TEST_URI).withSsl(true).build()).build().isSsl());
        Assertions.assertTrue(URIBuilder.of("rediss://localhost:12345").build().isSsl());
    }

}
