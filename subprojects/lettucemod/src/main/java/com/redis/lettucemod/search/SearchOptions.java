package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class SearchOptions<K, V> implements RediSearchArgument<K, V> {

	private boolean noContent;
	private boolean verbatim;
	private boolean noStopWords;
	private boolean withScores;
	private boolean withPayloads;
	private boolean withSortKeys;
	private List<NumericFilter<K, V>> filters = new ArrayList<>();
	private GeoFilter<K, V> geoFilter;
	private List<K> inKeys = new ArrayList<>();
	private List<K> inFields = new ArrayList<>();
	private List<K> returnFields = new ArrayList<>();
	private Summarize<K, V> summarize;
	private Highlight<K, V> highlight;
	private Long slop;
	private boolean inOrder;
	private Language language;
	private String expander;
	private String scorer;
	private V payload;
	private SortBy<K, V> sortBy;
	private Limit limit;

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

	public GeoFilter<K, V> getGeoFilter() {
		return geoFilter;
	}

	public void setGeoFilter(GeoFilter<K, V> geoFilter) {
		this.geoFilter = geoFilter;
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

	public Summarize<K, V> getSummarize() {
		return summarize;
	}

	public void setSummarize(Summarize<K, V> summarize) {
		this.summarize = summarize;
	}

	public Highlight<K, V> getHighlight() {
		return highlight;
	}

	public void setHighlight(Highlight<K, V> highlight) {
		this.highlight = highlight;
	}

	public Long getSlop() {
		return slop;
	}

	public void setSlop(Long slop) {
		this.slop = slop;
	}

	public boolean isInOrder() {
		return inOrder;
	}

	public void setInOrder(boolean inOrder) {
		this.inOrder = inOrder;
	}

	public Language getLanguage() {
		return language;
	}

	public void setLanguage(Language language) {
		this.language = language;
	}

	public String getExpander() {
		return expander;
	}

	public void setExpander(String expander) {
		this.expander = expander;
	}

	public String getScorer() {
		return scorer;
	}

	public void setScorer(String scorer) {
		this.scorer = scorer;
	}

	public V getPayload() {
		return payload;
	}

	public void setPayload(V payload) {
		this.payload = payload;
	}

	public SortBy<K, V> getSortBy() {
		return sortBy;
	}

	public void setSortBy(SortBy<K, V> sortBy) {
		this.sortBy = sortBy;
	}

	public Limit getLimit() {
		return limit;
	}

	public void setLimit(Limit limit) {
		this.limit = limit;
	}

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
		if (geoFilter != null) {
			args.add(SearchCommandKeyword.GEOFILTER);
			geoFilter.build(args);
		}
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
		if (summarize != null) {
			args.add(SearchCommandKeyword.SUMMARIZE);
			summarize.build(args);
		}
		if (highlight != null) {
			args.add(SearchCommandKeyword.HIGHLIGHT);
			highlight.build(args);
		}
		if (slop != null) {
			args.add(SearchCommandKeyword.SLOP);
			args.add(slop);
		}
		if (inOrder) {
			args.add(SearchCommandKeyword.INORDER);
		}
		if (language != null) {
			args.add(SearchCommandKeyword.LANGUAGE);
			args.add(language.getId());
		}
		if (expander != null) {
			args.add(SearchCommandKeyword.EXPANDER);
			args.add(expander);
		}
		if (scorer != null) {
			args.add(SearchCommandKeyword.SCORER);
			args.add(scorer);
		}
		if (payload != null) {
			args.add(SearchCommandKeyword.PAYLOAD);
			args.addValue(payload);
		}
		if (sortBy != null) {
			args.add(SearchCommandKeyword.SORTBY);
			sortBy.build(args);
		}
		if (limit != null) {
			limit.build(args);
		}
	}

	public static Limit limit(long offset, long num) {
		return new Limit(offset, num);
	}

	@SuppressWarnings("rawtypes")
	public static class Limit implements RediSearchArgument {

		private final long offset;
		private final long num;

		public Limit(long offset, long num) {
			this.offset = offset;
			this.num = num;
		}

		public static Limit of(long offset, long num) {
			return new Limit(offset, num);
		}

		@Override
		public void build(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.LIMIT);
			args.add(offset);
			args.add(num);
		}

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

		public static <K, V> NumericFilterBuilder<K, V> field(K field) {
			return new NumericFilterBuilder<>(field);
		}

		public static class NumericFilterBuilder<K, V> {

			private final K field;

			public NumericFilterBuilder(K field) {
				this.field = field;
			}

			public MinNumericFilterBuilder<K, V> min(double min) {
				return new MinNumericFilterBuilder<>(field, min);
			}

		}

		public static class MinNumericFilterBuilder<K, V> {

			private final K field;
			private final double min;

			public MinNumericFilterBuilder(K field, double min) {
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

		public static <K, V> GeoFilterBuilder<K, V> field(K field) {
			return new GeoFilterBuilder<>(field);
		}

		public static class GeoFilterBuilder<K, V> {

			private final K field;
			private double longitude;
			private double latitude;
			private double radius;
			private String unit;

			public GeoFilterBuilder(K field) {
				this.field = field;
			}

			public GeoFilterBuilder<K, V> longitude(double longitude) {
				this.longitude = longitude;
				return this;
			}

			public GeoFilterBuilder<K, V> latitude(double latitude) {
				this.latitude = latitude;
				return this;
			}

			public GeoFilterBuilder<K, V> radius(double radius) {
				this.radius = radius;
				return this;
			}

			public GeoFilterBuilder<K, V> unit(String unit) {
				this.unit = unit;
				return this;
			}

			public GeoFilter<K, V> build() {
				return new GeoFilter<>(field, longitude, latitude, radius, unit);
			}

		}
	}

	public static class Highlight<K, V> implements RediSearchArgument<K, V> {

		private List<K> fields = new ArrayList<>();
		private Tags<V> tags;

		private Highlight(Builder<K, V> builder) {
			this.fields = builder.fields;
			this.tags = builder.tags;
		}

		public static <K, V> Builder<K, V> builder() {
			return new Builder<>();
		}

		public static class Builder<K, V> {

			private List<K> fields = new ArrayList<>();
			private Tags<V> tags;

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
				this.tags = tags;
				return this;
			}

			public Builder<K, V> tags(V open, V close) {
				this.tags = new Tags<>(open, close);
				return this;
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
			if (tags != null) {
				args.add(SearchCommandKeyword.TAGS);
				args.addValue(tags.getOpen());
				args.addValue(tags.getClose());
			}
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
		private Long frags;
		private Long length;
		private V separator;

		public List<K> getFields() {
			return fields;
		}

		public void setFields(List<K> fields) {
			this.fields = fields;
		}

		public Long getFrags() {
			return frags;
		}

		public void setFrags(Long frags) {
			this.frags = frags;
		}

		public Long getLength() {
			return length;
		}

		public void setLength(Long length) {
			this.length = length;
		}

		public V getSeparator() {
			return separator;
		}

		public void setSeparator(V separator) {
			this.separator = separator;
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			if (!fields.isEmpty()) {
				args.add(SearchCommandKeyword.FIELDS);
				args.add(fields.size());
				fields.forEach(args::addKey);
			}
			if (frags != null) {
				args.add(SearchCommandKeyword.FRAGS);
				args.add(frags);
			}
			if (length != null) {
				args.add(SearchCommandKeyword.LEN);
				args.add(length);
			}
			if (separator != null) {
				args.add(SearchCommandKeyword.SEPARATOR);
				args.addValue(separator);
			}
		}

	}

	public static class SortBy<K, V> implements RediSearchArgument<K, V> {

		private final K field;
		private final Order direction;

		public SortBy(K field, Order direction) {
			this.field = field;
			this.direction = direction;
		}

		@Override
		public void build(SearchCommandArgs<K, V> args) {
			args.addKey(field);
			args.add(direction == Order.ASC ? SearchCommandKeyword.ASC : SearchCommandKeyword.DESC);
		}

		public static <K, V> SortByBuilder<K, V> field(K field) {
			return new SortByBuilder<>(field);
		}

		public static class SortByBuilder<K, V> {

			private final K field;

			public SortByBuilder(K field) {
				this.field = field;
			}

			public SortBy<K, V> order(Order order) {
				return new SortBy<>(field, order);
			}
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
		private GeoFilter<K, V> geoFilter;
		private List<K> inKeys = new ArrayList<>();
		private List<K> inFields = new ArrayList<>();
		private List<K> returnFields = new ArrayList<>();
		private Summarize<K, V> summarize;
		private Highlight<K, V> highlight;
		private Long slop;
		private boolean inOrder;
		private Language language;
		private String expander;
		private String scorer;
		private V payload;
		private SortBy<K, V> sortBy;
		private Limit limit;

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
			this.geoFilter = geoFilter;
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
			this.summarize = summarize;
			return this;
		}

		public Builder<K, V> highlight(Highlight<K, V> highlight) {
			this.highlight = highlight;
			return this;
		}

		public Builder<K, V> slop(Long slop) {
			this.slop = slop;
			return this;
		}

		public Builder<K, V> inOrder(boolean inOrder) {
			this.inOrder = inOrder;
			return this;
		}

		public Builder<K, V> language(Language language) {
			this.language = language;
			return this;
		}

		public Builder<K, V> expander(String expander) {
			this.expander = expander;
			return this;
		}

		public Builder<K, V> scorer(String scorer) {
			this.scorer = scorer;
			return this;
		}

		public Builder<K, V> payload(V payload) {
			this.payload = payload;
			return this;
		}

		public Builder<K, V> sortBy(SortBy<K, V> sortBy) {
			this.sortBy = sortBy;
			return this;
		}

		public Builder<K, V> limit(Limit limit) {
			this.limit = limit;
			return this;
		}

		public SearchOptions<K, V> build() {
			return new SearchOptions<>(this);
		}
	}
}
