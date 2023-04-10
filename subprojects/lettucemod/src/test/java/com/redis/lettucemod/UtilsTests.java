package com.redis.lettucemod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;

class UtilsTests {

	@Test
	void geoLocation() {
		double longitude = -118.753604;
		double latitude = 34.027201;
		String locationString = "-118.753604,34.027201";
		GeoLocation location = GeoLocation.of(locationString);
		Assertions.assertEquals(longitude, location.getLongitude());
		Assertions.assertEquals(latitude, location.getLatitude());
		Assertions.assertEquals(locationString,
				GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
	}

	@Test
	void redisURIBuilder() {
		String host = "streetlamp.lemousse.com";
		int port = 12345;
		String username = "peoek";
		char[] password = "secretpassword".toCharArray();
		String clientName = "redis-client";
		int database = 7;
		SslVerifyMode verifyPeer = SslVerifyMode.FULL;
		assertEquals(RedisURI.builder().withHost(host).withPort(port).withAuthentication(username, password)
				.withClientName(clientName).withDatabase(database).withVerifyPeer(verifyPeer).withSsl(true).build(),
				RedisURIBuilder.create().host(host).port(port).username(username).password(password)
						.clientName(clientName).ssl(true).database(database).sslVerifyMode(verifyPeer).build());
		assertEquals(RedisURI.builder().withHost(host).withPort(port).withSsl(true).build(),
				RedisURIBuilder.create("rediss://" + host + ":" + port).ssl(false).build());
		assertEquals(RedisURI.create("redis://myhost:12345"),
				RedisURIBuilder.create("redis://myhost:12345").port(12345).build());

	}

	@SuppressWarnings("deprecation")
	private void assertEquals(RedisURI expected, RedisURI actual) {
		Assertions.assertEquals(expected, actual);
		Assertions.assertEquals(expected.getClientName(), actual.getClientName());
		Assertions.assertEquals(expected.getUsername(), actual.getUsername());
		Assertions.assertArrayEquals(expected.getPassword(), actual.getPassword());
		Assertions.assertEquals(expected.isSsl(), actual.isSsl());
		Assertions.assertEquals(expected.isStartTls(), actual.isStartTls());
		Assertions.assertEquals(expected.isVerifyPeer(), actual.isVerifyPeer());
		Assertions.assertEquals(expected.getVerifyMode(), actual.getVerifyMode());
		Assertions.assertEquals(expected.getTimeout(), actual.getTimeout());

	}

}
