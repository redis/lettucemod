package com.redis.lettucemod.search;

import java.util.Objects;

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

	@Override
	public int hashCode() {
		return Objects.hash(count, maxIdle);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cursor other = (Cursor) obj;
		return Objects.equals(count, other.count) && Objects.equals(maxIdle, other.maxIdle);
	}

}
