package com.redis.search.query.impl;

import java.text.MessageFormat;

import com.redis.query.Field;

public class GeoRadiusCondition extends AbstractFieldCondition {

    private static final String FORMAT = "[{0} {1} {2} {3}]";

    private final GeoCoordinates coordinates;
    private final Distance radius;

    public GeoRadiusCondition(Field field, GeoCoordinates coordinates, Distance radius) {
	super(field);
	this.coordinates = coordinates;
	this.radius = radius;
    }

    @Override
    protected String valueString() {
	return MessageFormat.format(FORMAT, coordinates.getLon(), coordinates.getLat(), radius.getValue(),
		radius.getUnit().getString());
    }

}
