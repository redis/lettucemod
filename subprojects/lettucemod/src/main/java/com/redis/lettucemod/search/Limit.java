package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Limit implements AggregateOperation {

	private final long offset;
	private final long num;

	public Limit(long offset, long num) {
		this.offset = offset;
		this.num = num;
	}

	@Override
	public Type getType() {
		return Type.LIMIT;
	}

	public long getOffset() {
		return offset;
	}

	public long getNum() {
		return num;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.LIMIT);
		args.add(offset);
		args.add(num);
	}

	@Override
	public String toString() {
		return "LIMIT [offset=" + offset + ", num=" + num + "]";
	}

	public static NumBuilder offset(long offset) {
		return new Builder().offset(offset);
	}

	public interface OffsetBuilder {
		public NumBuilder offset(long offset);
	}

	public interface NumBuilder {
		public Limit num(long num);
	}

	public static final class Builder implements OffsetBuilder, NumBuilder {

		private long offset;

		private Builder() {
		}

		@Override
		public NumBuilder offset(long offset) {
			this.offset = offset;
			return this;
		}

		@Override
		public Limit num(long num) {
			return new Limit(offset, num);
		}

	}

}
