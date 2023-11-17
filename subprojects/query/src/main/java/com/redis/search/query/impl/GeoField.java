package com.redis.search.query.impl;

public class GeoField extends AbstractField {

    public GeoField(String name) {
	super(name);
    }

    public GeoRadiusCondition within(GeoCoordinates coordinates, Distance distance) {
	return new GeoRadiusCondition(this, coordinates, distance);
    }

}
