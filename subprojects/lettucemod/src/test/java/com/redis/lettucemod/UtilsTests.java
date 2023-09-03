package com.redis.lettucemod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.util.GeoLocation;

import io.lettuce.core.RedisURI;

class UtilsTests {

    @Test
    void geoLocation() {
        double longitude = -118.753604;
        double latitude = 34.027201;
        String locationString = "-118.753604,34.027201";
        GeoLocation location = GeoLocation.of(locationString);
        Assertions.assertEquals(longitude, location.getLongitude());
        Assertions.assertEquals(latitude, location.getLatitude());
        Assertions.assertEquals(locationString, GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
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
