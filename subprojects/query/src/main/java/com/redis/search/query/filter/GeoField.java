package com.redis.search.query.filter;

public class GeoField extends AbstractField {

    public GeoField(String name) {
	super(name);
    }

    public FieldCondition within(GeoCoordinates coordinates, Distance distance) {
	return new FieldCondition(this, new GeoCondition(coordinates, distance));
    }

}
