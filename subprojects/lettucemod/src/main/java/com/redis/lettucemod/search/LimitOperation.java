package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class LimitOperation implements AggregateOperation {

	private final long offset;
	private final long num;

	public LimitOperation(long offset, long num) {
		this.offset = offset;
		this.num = num;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.LIMIT);
		args.add(offset);
		args.add(num);
	}

	public static INumStage offset(long offset) {
		return new Builder().offset(offset);
	}

	public interface IOffsetStage {
		public INumStage offset(long offset);
	}

	public interface INumStage {
		public LimitOperation num(long num);
	}

	public static final class Builder implements IOffsetStage, INumStage {
		private long offset;

		private Builder() {
		}

		@Override
		public INumStage offset(long offset) {
			this.offset = offset;
			return this;
		}

		@Override
		public LimitOperation num(long num) {
			return new LimitOperation(offset, num);
		}

	}

}
