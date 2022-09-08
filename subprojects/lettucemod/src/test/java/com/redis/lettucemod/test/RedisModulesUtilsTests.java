package com.redis.lettucemod.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisClientBuilder;
import com.redis.lettucemod.util.RedisClientOptions;

import io.lettuce.core.RedisURI;

class RedisModulesUtilsTests {

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
	void clientBuilderURI() {
		RedisClientOptions.Builder options = RedisClientOptions.builder();
		String host = "streetlamp.lemousse.com";
		int port = 12345;
		options.host(host);
		options.port(port);
		String password = "secretpassword";
		options.password(password.toCharArray());
		RedisURI uri = RedisClientBuilder.create(options.build()).uri();
		Assertions.assertEquals(host, uri.getHost());
		Assertions.assertEquals(port, uri.getPort());
		Assertions.assertArrayEquals(password.toCharArray(),
				uri.getCredentialsProvider().resolveCredentials().block().getPassword());
	}

}
