package com.redis.search.query.impl;

import java.util.Collection;

public abstract class Assert {

    private Assert() {
    }

    public static void notNull(Object object, String message) {
	if (object == null) {
	    throw new IllegalArgumentException(message);
	}
    }

    public static void notEmpty(Collection<?> iterable, String message) {
	if (iterable == null || iterable.size() == 0) {
	    throw new IllegalArgumentException(message);
	}
    }

    public static void notEmpty(Object[] array, String message) {
	if (array == null || array.length == 0) {
	    throw new IllegalArgumentException(message);
	}
    }

    public static void notEmpty(int[] array, String message) {
	if (array == null || array.length == 0) {
	    throw new IllegalArgumentException(message);
	}
    }

    public static void isTrue(boolean value, String message) {
	if (!value) {
	    throw new IllegalArgumentException(message);
	}
    }

}
