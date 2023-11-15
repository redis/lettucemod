package com.redis.search.query.impl;

public class Distance {

    public enum Unit {

	KILOMETERS("km"), METERS("m"), FEET("ft"), MILES("mi");

	private String string;

	Unit(String string) {
	    this.string = string;
	}

	public String getString() {
	    return string;
	}
    }

    private final Number value;
    private final Unit unit;

    public Distance(Number value, Unit unit) {
	this.value = value;
	this.unit = unit;
    }

    public Number getValue() {
	return value;
    }

    public Unit getUnit() {
	return unit;
    }

    public static Distance kilometers(Number value) {
	return new Distance(value, Unit.KILOMETERS);
    }

    public static Distance miles(Number value) {
	return new Distance(value, Unit.MILES);
    }

    public static Distance feet(Number value) {
	return new Distance(value, Unit.FEET);
    }

    public static Distance meters(Number value) {
	return new Distance(value, Unit.METERS);
    }

    public static Distance of(Number value, Unit unit) {
	return new Distance(value, unit);
    }

}
