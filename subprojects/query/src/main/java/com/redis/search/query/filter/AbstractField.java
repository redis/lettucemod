package com.redis.search.query.filter;

public abstract class AbstractField implements Field {

    private final String name;

    protected AbstractField(String name) {
	this.name = name;
    }

    @Override
    public String getName() {
	return name;
    }

}
