package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class SearchOptions<K, V> implements RediSearchArgument<K, V> {

	private boolean noContent;
	private boolean verbatim;
	private boolean noStopWords;
	private boolean withScores;
	private boolean withPayloads;
	private boolean withSortKeys;
	private List<NumericFilter<K, V>> filters = new ArrayList<>();
	private Optional<GeoFilter<K, V>> geoFilter = Optional.empty();
	private List<K> inKeys = new ArrayList<>();
	private List<K> inFields = new ArrayList<>();
	private List<K> returnFields = new ArrayList<>();
	private Optional<Summarize<K, V>> summarize = Optional.empty();
	private Optional<Highlight<K, V>> highlight = Optional.empty();
	private Optional<Long> slop = Optional.empty();
	private boolean inOrder;
	private Optional<Language> language = Optional.empty();
	private Optional<String> expander = Optional.empty();
	private Optional<String> scorer = Optional.empty();
	private Optional<V> payload = Optional.empty();
	private Optional<SortBy<K, V>> sortBy = Optional.empty();
	private Optional<Limit> limit = Optional.empty();

	private SearchOptions(Builder<K, V> builder) {
		this.noContent = builder.noContent;
		this.verbatim = builder.verbatim;
		this.noStopWords = builder.noStopWords;
		this.withScores = builder.withScores;
		this.withPayloads = builder.withPayloads;
		this.withSortKeys = builder.withSortKeys;
		this.filters = builder.filters;
		this.geoFilter = builder.geoFilter;
		this.inKeys = builder.inKeys;
		this.inFields = builder.inFields;
		this.returnFields = builder.returnFields;
		this.summarize = builder.summarize;
		this.highlight = builder.highlight;
		this.slop = builder.slop;
		this.inOrder = builder.inOrder;
		this.language = builder.language;
		this.expander = builder.expander;
		this.scorer = builder.scorer;
		this.payload = builder.payload;
		this.sortBy = builder.sortBy;
		this.limit = builder.limit;
	}

	public boolean isNoContent() {
		return noContent;
	}

	public void setNoContent(boolean noContent) {
		this.noContent = noContent;
	}

	public boolean isVerbatim() {
		return verbatim;
	}

	public void setVerbatim(boolean verbatim) {
		this.verbatim = verbatim;
	}

	public boolean isNoStopWords() {
		return noStopWords;
	}

	public void setNoStopWords(boolean noStopWords) {
		this.noStopWords = noStopWords;
	}

	public boolean isWithScores() {
		return withScores;
	}

	public void setWithScores(boolean withScores) {
		this.withScores = withScores;
	}

	public boolean isWithPayloads() {
		return withPayloads;
	}

	public void setWithPayloads(boolean withPayloads) {
		this.withPayloads = withPayloads;
	}

	public boolean isWithSortKeys() {
		return withSortKeys;
	}

	public void setWithSortKeys(boolean withSortKeys) {
		this.withSortKeys = withSortKeys;
	}

	public List<NumericFilter<K, V>> getFilters() {
		return filters;
	}

	public void setFilters(List<NumericFilter<K, V>> filters) {
		this.filters = filters;
	}

	public Optional<GeoFilter<K, V>> getGeoFilter() {
		return geoFilter;
	}

	public void setGeoFilter(GeoFilter<K, V> geoFilter) {
		this.geoFilter = Optional.of(geoFilter);
	}

	public List<K> getInKeys() {
		return inKeys;
	}

	public void setInKeys(List<K> inKeys) {
		this.inKeys = inKeys;
	}

	public List<K> getInFields() {
		return inFields;
	}

	public void setInFields(List<K> inFields) {
		this.inFields = inFields;
	}

	public List<K> getReturnFields() {
		return returnFields;
	}

	public void setReturnFields(List<K> returnFields) {
		this.returnFields = returnFields;
	}

	public Optional<Summarize<K, V>> getSummarize() {
		return summarize;
	}

	public void setSummarize(Summarize<K, V> summarize) {
		this.summarize = Optional.of(summarize);
	}

	public Optional<Highlight<K, V>> getHighlight() {
		return highlight;
	}

	public void setHighlight(Highlight<K, V> highlight) {
		this.highlight = Optional.of(highlight);
	}

	public Optional<Long> getSlop() {
		return slop;
	}

	public void setSlop(long slop) {
		this.slop = Optional.of(slop);
	}

	public boolean isInOrder() {
		return inOrder;
	}

	public void setInOrder(boolean inOrder) {
		this.inOrder = inOrder;
	}

	public Optional<Language> getLanguage() {
		return language;
	}

	public void setLanguage(Language language) {
		this.language = Optional.of(language);
	}

	public Optional<String> getExpander() {
		return expander;
	}

	public void setExpander(String expander) {
		this.expander = Optional.of(expander);
	}

	public Optional<String> getScorer() {
		return scorer;
	}

	public void setScorer(String scorer) {
		this.scorer = Optional.of(scorer);
	}

	public Optional<V> getPayload() {
		return payload;
	}

	public void setPayload(V payload) {
		this.payload = Optional.of(payload);
	}

	public Optional<SortBy<K, V>> getSortBy() {
		return sortBy;
	}

	public void setSortBy(SortBy<K, V> sortBy) {
		this.sortBy = Optional.of(sortBy);
	}

	public Optional<Limit> getLimit() {
		return limit;
	}

	public void setLimit(Limit limit) {
		this.limit = Optional.of(limit);
	}

	@Override
	public String toString() {
		return "SearchOptions [noContent=" + noContent + ", verbatim=" + verbatim + ", noStopWords=" + noStopWords
				+ ", withScores=" + withScores + ", withPayloads=" + withPayloads + ", withSortKeys=" + withSortKeys
				+ ", filters=" + filters + ", geoFilter=" + geoFilter + ", inKeys=" + inKeys + ", inFields=" + inFields
				+ ", returnFields=" + returnFields + ", summarize=" + summarize + ", highlight=" + highlight + ", slop="
				+ slop + ", inOrder=" + inOrder + ", language=" + language + ", expander=" + expander + ", scorer="
				+ scorer + ", payload=" + payload + ", sortBy=" + sortBy + ", limit=" + limit + "]";
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (noContent) {
			args.add(SearchCommandKeyword.NOCONTENT);
		}
		if (verbatim) {
			args.add(SearchCommandKeyword.VERBATIM);
		}
		if (noStopWords) {
			args.add(SearchCommandKeyword.NOSTOPWORDS);
		}
		if (withScores) {
			args.add(SearchCommandKeyword.WITHSCORES);
		}
		if (withPayloads) {
			args.add(SearchCommandKeyword.WITHPAYLOADS);
		}
		if (withSortKeys) {
			args.add(SearchCommandKeyword.WITHSORTKEYS);
		}
		for (NumericFilter<K, V> filter : filters) {
			args.add(SearchCommandKeyword.FILTER);
			filter.build(args);
		}
		geoFilter.ifPresent(f -> {
			args.add(SearchCommandKeyword.GEOFILTER);
			f.build(args);
		});
		if (!inKeys.isEmpty()) {
			args.add(SearchCommandKeyword.INKEYS);
			args.add(inKeys.size());
			inKeys.forEach(args::addKey);
		}
		if (!inFields.isEmpty()) {
			args.add(SearchCommandKeyword.INFIELDS);
			args.add(inFields.size());
			inFields.forEach(args::addKey);
		}
		if (!returnFields.isEmpty()) {
			args.add(SearchCommandKeyword.RETURN);
			args.add(returnFields.size());
			returnFields.forEach(args::addKey);
		}
		summarize.ifPresent(s -> {
			args.add(SearchCommandKeyword.SUMMARIZE);
			s.build(args);
		});
		highlight.ifPresent(h -> {
			args.add(SearchCommandKeyword.HIGHLIGHT);
			h.build(args);
		});
		slop.ifPresent(s -> args.add(SearchCommandKeyword.SLOP).add(s));
		if (inOrder) {
			args.add(SearchCommandKeyword.INORDER);
		}
		language.ifPresent(l -> args.add(SearchCommandKeyword.LANGUAGE).add(l.getId()));
		expander.ifPresent(e -> args.add(SearchCommandKeyword.EXPANDER).add(e));
		scorer.ifPresent(s -> args.add(SearchCommandKeyword.SCORER).add(s));
		payload.ifPresent(p -> args.add(SearchCommandKeyword.PAYLOAD).addValue(p));
		sortBy.ifPresent(s -> s.build(args));
		limit.ifPresent(l -> l.build((SearchCommandArgs) args));
	}

	public static Limit limit(long offset, long num) {
		return new Limit(offset, num);
	}

	public static class NumericFilter<K, V> implements RediSearchArgument<K, V> {

		private final K field;
		private final double min;
		private final double max;

		public NumericFilter(K field, double min, double max) {
			this.field = field;
			this.min = min;
			this.max = max;
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			args.addKey(field);
			args.add(min);
			args.add(max);
		}

		public static <K, V> Builder<K, V> field(K field) {
			return new Builder<>(field);
		}

		public static class Builder<K, V> {

			private final K field;

			public Builder(K field) {
				this.field = field;
			}

			public MaxNumericFilterBuilder<K, V> min(double min) {
				return new MaxNumericFilterBuilder<>(field, min);
			}

		}

		public static class MaxNumericFilterBuilder<K, V> {

			private final K field;
			private final double min;

			public MaxNumericFilterBuilder(K field, double min) {
				this.field = field;
				this.min = min;
			}

			public NumericFilter<K, V> max(double max) {
				return new NumericFilter<>(field, min, max);
			}
		}
	}

	public static class GeoFilter<K, V> implements RediSearchArgument<K, V> {

		private K field;
		private double longitude;
		private double latitude;
		private double radius;
		private String unit;

		public GeoFilter() {
		}

		public GeoFilter(K field, double longitude, double latitude, double radius, String unit) {
			this.field = field;
			this.longitude = longitude;
			this.latitude = latitude;
			this.radius = radius;
			this.unit = unit;
		}

		public K getField() {
			return field;
		}

		public void setField(K field) {
			this.field = field;
		}

		public double getLongitude() {
			return longitude;
		}

		public void setLongitude(double longitude) {
			this.longitude = longitude;
		}

		public double getLatitude() {
			return latitude;
		}

		public void setLatitude(double latitude) {
			this.latitude = latitude;
		}

		public double getRadius() {
			return radius;
		}

		public void setRadius(double radius) {
			this.radius = radius;
		}

		public String getUnit() {
			return unit;
		}

		public void setUnit(String unit) {
			this.unit = unit;
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			args.addKey(field);
			args.add(longitude);
			args.add(latitude);
			args.add(radius);
			args.add(unit);
		}

		public static <K, V> Builder<K, V> field(K field) {
			return new Builder<>(field);
		}

		public static class Builder<K, V> {

			private final K field;
			private double longitude;
			private double latitude;
			private double radius;
			private String unit;

			public Builder(K field) {
				this.field = field;
			}

			public Builder<K, V> longitude(double longitude) {
				this.longitude = longitude;
				return this;
			}

			public Builder<K, V> latitude(double latitude) {
				this.latitude = latitude;
				return this;
			}

			public Builder<K, V> radius(double radius) {
				this.radius = radius;
				return this;
			}

			public Builder<K, V> unit(String unit) {
				this.unit = unit;
				return this;
			}

			public GeoFilter<K, V> build() {
				return new GeoFilter<>(field, longitude, latitude, radius, unit);
			}

		}
	}

	public static class Highlight<K, V> implements RediSearchArgument<K, V> {

		private final List<K> fields;
		private final Optional<Tags<V>> tags;

		private Highlight(Builder<K, V> builder) {
			this.fields = builder.fields;
			this.tags = builder.tags;
		}

		public static <K, V> Builder<K, V> builder() {
			return new Builder<>();
		}

		public static class Builder<K, V> {

			private List<K> fields = new ArrayList<>();
			private Optional<Tags<V>> tags = Optional.empty();

			public Builder<K, V> field(K field) {
				this.fields.add(field);
				return this;
			}

			@SuppressWarnings("unchecked")
			public Builder<K, V> fields(K... fields) {
				this.fields.addAll(Arrays.asList(fields));
				return this;
			}

			public Builder<K, V> tags(Tags<V> tags) {
				this.tags = Optional.of(tags);
				return this;
			}

			public Builder<K, V> tags(V open, V close) {
				return tags(new Tags<>(open, close));
			}

			public Highlight<K, V> build() {
				return new Highlight<>(this);
			}

		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			if (!fields.isEmpty()) {
				args.add(SearchCommandKeyword.FIELDS);
				args.add(fields.size());
				fields.forEach(args::addKey);
			}
			tags.ifPresent(t -> args.add(SearchCommandKeyword.TAGS).addValue(t.getOpen()).addValue(t.getClose()));
		}

		public static class Tags<V> {

			private V open;
			private V close;

			public Tags(V open, V close) {
				super();
				this.open = open;
				this.close = close;
			}

			public V getOpen() {
				return open;
			}

			public void setOpen(V open) {
				this.open = open;
			}

			public V getClose() {
				return close;
			}

			public void setClose(V close) {
				this.close = close;
			}

		}

	}

	public static class Summarize<K, V> implements RediSearchArgument<K, V> {

		private List<K> fields = new ArrayList<>();
		private OptionalLong frags = OptionalLong.empty();
		private OptionalLong length = OptionalLong.empty();
		private Optional<V> separator = Optional.empty();

		public List<K> getFields() {
			return fields;
		}

		public void setFields(List<K> fields) {
			this.fields = fields;
		}

		public OptionalLong getFrags() {
			return frags;
		}

		public void setFrags(long frags) {
			this.frags = OptionalLong.of(frags);
		}

		public OptionalLong getLength() {
			return length;
		}

		public void setLength(long length) {
			this.length = OptionalLong.of(length);
		}

		public Optional<V> getSeparator() {
			return separator;
		}

		public void setSeparator(V separator) {
			this.separator = Optional.of(separator);
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			if (!fields.isEmpty()) {
				args.add(SearchCommandKeyword.FIELDS);
				args.add(fields.size());
				fields.forEach(args::addKey);
			}
			frags.ifPresent(f -> args.add(SearchCommandKeyword.FRAGS).add(f));
			length.ifPresent(l -> args.add(SearchCommandKeyword.LEN).add(l));
			separator.ifPresent(s -> args.add(SearchCommandKeyword.SEPARATOR).addValue(s));
		}
	}

	public static class SortBy<K, V> implements RediSearchArgument<K, V> {

		private final K field;
		private final Order direction;

		public SortBy(K field, Order direction) {
			this.field = field;
			this.direction = direction;
		}

		public K getField() {
			return field;
		}

		public Order getDirection() {
			return direction;
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			args.add(SearchCommandKeyword.SORTBY).addKey(field);
			args.add(direction.getKeyword());
		}

		public static <K, V> SortBy<K, V> asc(K field) {
			return new SortBy<>(field, Order.ASC);
		}

		public static <K, V> SortBy<K, V> desc(K field) {
			return new SortBy<>(field, Order.DESC);
		}

	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static final class Builder<K, V> {
		private boolean noContent;
		private boolean verbatim;
		private boolean noStopWords;
		private boolean withScores;
		private boolean withPayloads;
		private boolean withSortKeys;
		private List<NumericFilter<K, V>> filters = new ArrayList<>();
		private Optional<GeoFilter<K, V>> geoFilter = Optional.empty();
		private List<K> inKeys = new ArrayList<>();
		private List<K> inFields = new ArrayList<>();
		private List<K> returnFields = new ArrayList<>();
		private Optional<Summarize<K, V>> summarize = Optional.empty();
		private Optional<Highlight<K, V>> highlight = Optional.empty();
		private Optional<Long> slop = Optional.empty();
		private boolean inOrder;
		private Optional<Language> language = Optional.empty();
		private Optional<String> expander = Optional.empty();
		private Optional<String> scorer = Optional.empty();
		private Optional<V> payload = Optional.empty();
		private Optional<SortBy<K, V>> sortBy = Optional.empty();
		private Optional<Limit> limit = Optional.empty();

		private Builder() {
		}

		public Builder<K, V> noContent(boolean noContent) {
			this.noContent = noContent;
			return this;
		}

		public Builder<K, V> verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return this;
		}

		public Builder<K, V> noStopWords(boolean noStopWords) {
			this.noStopWords = noStopWords;
			return this;
		}

		public Builder<K, V> withScores(boolean withScores) {
			this.withScores = withScores;
			return this;
		}

		public Builder<K, V> withPayloads(boolean withPayloads) {
			this.withPayloads = withPayloads;
			return this;
		}

		public Builder<K, V> withSortKeys(boolean withSortKeys) {
			this.withSortKeys = withSortKeys;
			return this;
		}

		public Builder<K, V> filter(NumericFilter<K, V> filter) {
			this.filters.add(filter);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> filters(NumericFilter<K, V>... filters) {
			this.filters.addAll(Arrays.asList(filters));
			return this;
		}

		public Builder<K, V> geoFilter(GeoFilter<K, V> geoFilter) {
			this.geoFilter = Optional.of(geoFilter);
			return this;
		}

		public Builder<K, V> inKey(K inKey) {
			this.inKeys.add(inKey);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> inKeys(K... inKeys) {
			this.inKeys.addAll(Arrays.asList(inKeys));
			return this;
		}

		public Builder<K, V> inField(K inField) {
			this.inFields.add(inField);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> inFields(K... inFields) {
			this.inFields.addAll(Arrays.asList(inFields));
			return this;
		}

		public Builder<K, V> returnField(K returnField) {
			this.returnFields.add(returnField);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> returnFields(K... returnFields) {
			this.returnFields.addAll(Arrays.asList(returnFields));
			return this;
		}

		public Builder<K, V> summarize(Summarize<K, V> summarize) {
			this.summarize = Optional.of(summarize);
			return this;
		}

		public Builder<K, V> highlight(Highlight<K, V> highlight) {
			this.highlight = Optional.of(highlight);
			return this;
		}

		public Builder<K, V> slop(long slop) {
			this.slop = Optional.of(slop);
			return this;
		}

		public Builder<K, V> inOrder(boolean inOrder) {
			this.inOrder = inOrder;
			return this;
		}

		public Builder<K, V> language(Language language) {
			this.language = Optional.of(language);
			return this;
		}

		public Builder<K, V> expander(String expander) {
			this.expander = Optional.of(expander);
			return this;
		}

		public Builder<K, V> scorer(String scorer) {
			this.scorer = Optional.of(scorer);
			return this;
		}

		public Builder<K, V> payload(V payload) {
			this.payload = Optional.of(payload);
			return this;
		}

		public Builder<K, V> sortBy(SortBy<K, V> sortBy) {
			this.sortBy = Optional.of(sortBy);
			return this;
		}

		public Builder<K, V> limit(Limit limit) {
			this.limit = Optional.of(limit);
			return this;
		}

		public SearchOptions<K, V> build() {
			return new SearchOptions<>(this);
		}
	}
}
