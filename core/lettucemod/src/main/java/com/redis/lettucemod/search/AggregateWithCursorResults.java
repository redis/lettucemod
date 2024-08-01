package com.redis.lettucemod.search;

import java.util.Objects;

public class AggregateWithCursorResults<K> extends AggregateResults<K> {

	private static final long serialVersionUID = 1L;

	private long cursor;

	public long getCursor() {
		return cursor;
	}

	public void setCursor(long cursor) {
		this.cursor = cursor;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(cursor);
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		AggregateWithCursorResults<K> other = (AggregateWithCursorResults<K>) obj;
		return cursor == other.cursor;
	}

}
