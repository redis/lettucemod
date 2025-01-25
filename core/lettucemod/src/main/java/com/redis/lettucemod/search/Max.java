package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

import lombok.ToString;

@SuppressWarnings("rawtypes")
@ToString
public class Max implements RediSearchArgument {

	private final long value;

	public Max(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.MAX).add(value);
	}

}
