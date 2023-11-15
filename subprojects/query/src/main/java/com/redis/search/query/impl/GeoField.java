package com.redis.search.query.impl;

public class GeoField extends AbstractField {

    public GeoField(String name) {
	super(name);
    }

    public GeoFieldCondition within(GeoCoordinates coordinates, Distance distance) {
	return new GeoFieldCondition(this, coordinates, distance);
    }

}
