package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class As implements RediSearchArgument {

	private final String field;

	public As(String field) {
		this.field = field;
	}

	public String getField() {
		return field;
	}

	public static As of(String field) {
		return new As(field);
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.AS).add(field);
	}

	@Override
	public String toString() {
		return "AS " + field;
	}

}
