package com.redis.lettucemod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisURIBuilder;

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
		RedisURIBuilder uriBuilder = RedisURIBuilder.create();
		String host = "streetlamp.lemousse.com";
		int port = 12345;
		uriBuilder.host(host);
		uriBuilder.port(port);
		char[] password = "secretpassword".toCharArray();
		uriBuilder.password(password);
		RedisURI uri = uriBuilder.build();
		Assertions.assertEquals(host, uri.getHost());
		Assertions.assertEquals(port, uri.getPort());
		Assertions.assertArrayEquals(password, uri.getCredentialsProvider().resolveCredentials().block().getPassword());
	}

}
