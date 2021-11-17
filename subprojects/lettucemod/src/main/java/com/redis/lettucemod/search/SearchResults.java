package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Objects;

public class SearchResults<K, V> extends ArrayList<Document<K, V>> {

	private static final long serialVersionUID = 1L;

	private long count;

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(count);
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SearchResults other = (SearchResults) obj;
		return count == other.count;
	}

}
