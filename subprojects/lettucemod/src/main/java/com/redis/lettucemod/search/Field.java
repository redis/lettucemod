package com.redis.lettucemod.search;

import java.util.Objects;
import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public abstract class Field<K> implements RediSearchArgument {

	public enum Type {
		TEXT, NUMERIC, GEO, TAG, VECTOR
	}

	protected final Type type;
	protected final K name;
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

	public void setAs(K as) {
		this.as = Optional.of(as);
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
	public int hashCode() {
		return Objects.hash(as, name, noIndex, sortable, type, unNormalizedForm);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Field<?> other = (Field<?>) obj;
		return Objects.equals(as, other.as) && Objects.equals(name, other.name) && noIndex == other.noIndex
				&& sortable == other.sortable && type == other.type && unNormalizedForm == other.unNormalizedForm;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void build(SearchCommandArgs args) {
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
			return sortable(true);
		}

		public B sortable(boolean sortable) {
			this.sortable = sortable;
			return (B) this;
		}

		public B unNormalizedForm() {
			return unNormalizedForm(true);
		}

		public B unNormalizedForm(boolean unf) {
			this.sortable = unf;
			this.unNormalizedForm = unf;
			return (B) this;
		}

		public B noIndex() {
			return noIndex(true);
		}

		public B noIndex(boolean noIndex) {
			this.noIndex = noIndex;
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

    public static <K> VectorField.Builder<K> vector(K name) {
		return VectorField.name(name);
	}
}
