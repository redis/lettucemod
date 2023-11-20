package com.redis.search.query.filter;

public class GeoCoordinates {

    private final Number lon;
    private final Number lat;

    public GeoCoordinates(Number lon, Number lat) {
	Utils.notNull(lon, "Longitude must not be null");
	Utils.notNull(lat, "Latitude must not be null");
	this.lon = lon;
	this.lat = lat;
    }

    public static Builder lon(Number lon) {
	return new Builder(lon);
    }

    public Number getLon() {
	return lon;
    }

    public Number getLat() {
	return lat;
    }

    public static class Builder {

	private final Number lon;

	public Builder(Number lon) {
	    this.lon = lon;
	}

	public GeoCoordinates lat(Number lat) {
	    return new GeoCoordinates(lon, lat);
	}

    }

}
