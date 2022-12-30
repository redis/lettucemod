package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Max implements RediSearchArgument {

	private final long value;

	public Max(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "Max [value=" + value + "]";
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.MAX).add(value);
	}

}
