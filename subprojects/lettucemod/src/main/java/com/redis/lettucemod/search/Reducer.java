package com.redis.lettucemod.search;

import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public abstract class Reducer implements RediSearchArgument {

	protected final Optional<As> as;

	protected Reducer(Optional<As> as) {
		this.as = as;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.REDUCE);
		buildFunction(args);
		as.ifPresent(a -> a.build(args));
	}

	protected String toString(String string) {
		if (as.isPresent()) {
			return string + " AS " + as.get().getField();
		}
		return string;
	}

	protected abstract void buildFunction(SearchCommandArgs args);

	@SuppressWarnings("unchecked")
	public static class Builder<B extends Builder<B>> {

		protected Optional<As> as = Optional.empty();

		public B as(String as) {
			this.as = Optional.of(new As(as));
			return (B) this;
		}

	}

}
