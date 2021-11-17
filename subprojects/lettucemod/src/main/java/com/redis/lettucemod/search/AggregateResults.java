package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

public class AggregateResults<K> extends ArrayList<Map<K, Object>> {

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

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		AggregateResults<K> other = (AggregateResults<K>) obj;
		return count == other.count;
	}
	
	

}
