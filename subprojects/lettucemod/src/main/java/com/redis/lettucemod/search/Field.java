package com.redis.lettucemod.search;

import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public abstract class Field<K> implements RediSearchArgument<K, Object> {

	private final Type type;
	private final K name;
	protected Optional<K> as = Optional.empty();
	protected boolean sortable;
	protected boolean unNormalizedForm;
	protected boolean noIndex;

	protected Field(Type type, Builder<K, ?> builder) {
		this.type = type;
		this.name = builder.name;
		this.as = builder.as;
		this.sortable = builder.sortable;
		this.unNormalizedForm = builder.unNormalizedForm;
		this.noIndex = builder.noIndex;
	}

	public Type getType() {
		return type;
	}

	public K getName() {
		return name;
	}

	public Optional<K> getAs() {
		return as;
	}

	public boolean isSortable() {
		return sortable;
	}

	public void setSortable(boolean sortable) {
		this.sortable = sortable;
	}

	public boolean isUnNormalizedForm() {
		return unNormalizedForm;
	}

	public void setUnNormalizedForm(boolean unNormalizedForm) {
		this.unNormalizedForm = unNormalizedForm;
	}

	public boolean isNoIndex() {
		return noIndex;
	}

	public void setNoIndex(boolean noIndex) {
		this.noIndex = noIndex;
	}

	@Override
	public void build(SearchCommandArgs<K, Object> args) {
		args.addKey(name);
		as.ifPresent(a -> args.add(SearchCommandKeyword.AS).addKey(a));
		buildField(args);
		if (sortable) {
			args.add(SearchCommandKeyword.SORTABLE);
			if (unNormalizedForm) {
				args.add(SearchCommandKeyword.UNF);
			}
		}
		if (noIndex) {
			args.add(SearchCommandKeyword.NOINDEX);
		}
	}

	protected abstract void buildField(SearchCommandArgs<K, Object> args);

	@SuppressWarnings("unchecked")
	public static class Builder<K, B extends Builder<K, B>> {

		protected final K name;
		private Optional<K> as = Optional.empty();
		private boolean sortable;
		private boolean unNormalizedForm;
		private boolean noIndex;

		protected Builder(K name) {
			this.name = name;
		}

		public B as(K as) {
			this.as = Optional.of(as);
			return (B) this;
		}

		public B sortable() {
			this.sortable = true;
			return (B) this;
		}

		public B unNormalizedForm() {
			this.sortable = true;
			this.unNormalizedForm = true;
			return (B) this;
		}

		public B noIndex() {
			this.noIndex = true;
			return (B) this;
		}

	}

	public static <K> TextField.Builder<K> text(K name) {
		return TextField.name(name);
	}

	public static <K> GeoField.Builder<K> geo(K name) {
		return GeoField.name(name);
	}

	public static <K> TagField.Builder<K> tag(K name) {
		return TagField.name(name);
	}

	public static <K> NumericField.Builder<K> numeric(K name) {
		return NumericField.name(name);
	}

	public enum Type {
		TEXT, NUMERIC, GEO, TAG
	}

}
