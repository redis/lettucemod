package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Max implements RediSearchArgument {

	private final long max;

	public Max(long max) {
		this.max = max;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.MAX).add(max);
	}

}
