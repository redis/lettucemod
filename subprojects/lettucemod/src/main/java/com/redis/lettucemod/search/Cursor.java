package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class Cursor {

	private Long count;
	private Long maxIdle;

	public Cursor() {
	}

	public Cursor(Long count, Long maxIdle) {
		this.count = count;
		this.maxIdle = maxIdle;
	}

	public <K, V> void build(SearchCommandArgs<K, V> args) {
		if (count != null) {
			args.add(SearchCommandKeyword.COUNT);
			args.add(count);
		}
		if (maxIdle != null) {
			args.add(SearchCommandKeyword.MAXIDLE);
			args.add(maxIdle);
		}
	}

}
