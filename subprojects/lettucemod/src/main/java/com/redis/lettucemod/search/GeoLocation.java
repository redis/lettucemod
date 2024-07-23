package com.redis.lettucemod.search;

import io.lettuce.core.internal.LettuceAssert;

public class GeoLocation {

	public static final String SEPARATOR = ",";

	private double longitude;
	private double latitude;

	public GeoLocation(double longitude, double latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public static GeoLocation of(String location) {
		LettuceAssert.notNull(location, "Location string must not be null");
		String[] lonlat = location.split(SEPARATOR);
		LettuceAssert.isTrue(lonlat.length == 2, "Location string not in proper format \"longitude,latitude\"");
		double longitude = Double.parseDouble(lonlat[0]);
		double latitude = Double.parseDouble(lonlat[1]);
		return new GeoLocation(longitude, latitude);
	}

	public static String toString(String longitude, String latitude) {
		if (longitude == null || latitude == null) {
			return null;
		}
		return longitude + SEPARATOR + latitude;
	}

}