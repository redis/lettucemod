package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public abstract class Reducer implements RediSearchArgument {

	private final String as;

	protected Reducer(String as) {
		this.as = as;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.REDUCE);
		buildFunction(args);
		if (as != null) {
			args.add(SearchCommandKeyword.AS);
			args.add(as);
		}
	}

	protected abstract void buildFunction(SearchCommandArgs args);

	@SuppressWarnings("unchecked")
	public static class ReducerBuilder<B extends ReducerBuilder<B>> {

		protected String as;

		public B as(String as) {
			this.as = as;
			return (B) this;
		}

	}

}
