package com.redis.lettucemod.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.RedisModulesUtils;

class RedisModulesUtilsTests {

	@Test
	void geoLocation() {
		double longitude = -118.753604;
		double latitude = 34.027201;
		String locationString = "-118.753604,34.027201";
		RedisModulesUtils.GeoLocation location = RedisModulesUtils.GeoLocation.of(locationString);
		Assertions.assertEquals(longitude, location.getLongitude());
		Assertions.assertEquals(latitude, location.getLatitude());
		Assertions.assertEquals(locationString,
				RedisModulesUtils.GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
	}
}
