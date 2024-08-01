package com.redis.search.query.filter;

public class GeoCondition implements Condition {

    private static final String FORMAT = "[%s %s %s %s]";
    private final GeoCoordinates coordinates;
    private final Distance radius;

    public GeoCondition(GeoCoordinates coordinates, Distance radius) {
	this.coordinates = coordinates;
	this.radius = radius;
    }

    @Override
    public String getQuery() {
	return String.format(FORMAT, coordinates.getLon(), coordinates.getLat(), radius.getValue(),
		radius.getUnit().getString());
    }

}
