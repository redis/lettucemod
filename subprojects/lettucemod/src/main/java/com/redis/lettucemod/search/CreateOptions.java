package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class CreateOptions<K, V> implements RediSearchArgument<K, V> {

	public enum DataType {
		HASH, JSON
	}

	private DataType on;
	private List<K> prefixes = new ArrayList<>();
	private V filter;
	private Language defaultLanguage;
	private K languageField;
	private Double defaultScore;
	private K scoreField;
	private K payloadField;
	private boolean maxTextFields;
	private Long temporary;
	private boolean noOffsets;
	private boolean noHL;
	private boolean noFields;
	private boolean noFreqs;
	private boolean noItitialScan;
	/**
	 * Set this to empty list for STOPWORDS 0
	 */
	private List<V> stopWords;

	private CreateOptions(Builder<K, V> builder) {
		this.on = builder.on;
		this.prefixes = builder.prefixes;
		this.filter = builder.filter;
		this.defaultLanguage = builder.defaultLanguage;
		this.languageField = builder.languageField;
		this.defaultScore = builder.defaultScore;
		this.scoreField = builder.scoreField;
		this.payloadField = builder.payloadField;
		this.maxTextFields = builder.maxTextFields;
		this.temporary = builder.temporary;
		this.noOffsets = builder.noOffsets;
		this.noHL = builder.noHL;
		this.noFields = builder.noFields;
		this.noFreqs = builder.noFreqs;
		this.noItitialScan = builder.noItitialScan;
		this.stopWords = builder.stopWords;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (on != null) {
			args.add(SearchCommandKeyword.ON);
			args.add(on.name());
		}
		if (prefixes != null && !prefixes.isEmpty()) {
			args.add(SearchCommandKeyword.PREFIX);
			args.add(prefixes.size());
			prefixes.forEach(args::addKey);
		}
		if (filter != null) {
			args.add(SearchCommandKeyword.FILTER);
			args.addValue(filter);
		}
		if (defaultLanguage != null) {
			args.add(SearchCommandKeyword.LANGUAGE);
			args.add(defaultLanguage.getId());
		}
		if (languageField != null) {
			args.add(SearchCommandKeyword.LANGUAGE_FIELD);
			args.addKey(languageField);
		}
		if (defaultScore != null) {
			args.add(SearchCommandKeyword.SCORE);
			args.add(defaultScore);
		}
		if (scoreField != null) {
			args.add(SearchCommandKeyword.SCORE_FIELD);
			args.addKey(scoreField);
		}
		if (payloadField != null) {
			args.add(SearchCommandKeyword.PAYLOAD_FIELD);
			args.addKey(payloadField);
		}
		if (maxTextFields) {
			args.add(SearchCommandKeyword.MAXTEXTFIELDS);
		}
		if (temporary != null) {
			args.add(SearchCommandKeyword.TEMPORARY);
			args.add(temporary);
		}
		if (noOffsets) {
			args.add(SearchCommandKeyword.NOOFFSETS);
		}
		if (noHL) {
			args.add(SearchCommandKeyword.NOHL);
		}
		if (noFields) {
			args.add(SearchCommandKeyword.NOFIELDS);
		}
		if (noFreqs) {
			args.add(SearchCommandKeyword.NOFREQS);
		}
		if (noItitialScan) {
			args.add(SearchCommandKeyword.NOINITIALSCAN);
		}
		if (stopWords != null) {
			args.add(SearchCommandKeyword.STOPWORDS);
			args.add(stopWords.size());
			stopWords.forEach(args::addValue);
		}
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static final class Builder<K, V> {

		private DataType on = DataType.HASH;
		private List<K> prefixes = new ArrayList<>();
		private V filter;
		private Language defaultLanguage;
		private K languageField;
		private Double defaultScore;
		private K scoreField;
		private K payloadField;
		private boolean maxTextFields;
		private Long temporary;
		private boolean noOffsets;
		private boolean noHL;
		private boolean noFields;
		private boolean noFreqs;
		private boolean noItitialScan;
		private List<V> stopWords = new ArrayList<>();

		public Builder<K, V> on(DataType on) {
			this.on = on;
			return this;
		}

		public Builder<K, V> prefix(K prefix) {
			this.prefixes.add(prefix);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> prefixes(K... prefixes) {
			this.prefixes.addAll(Arrays.asList(prefixes));
			return this;
		}

		public Builder<K, V> filter(V filter) {
			this.filter = filter;
			return this;
		}

		public Builder<K, V> defaultLanguage(Language defaultLanguage) {
			this.defaultLanguage = defaultLanguage;
			return this;
		}

		public Builder<K, V> languageField(K languageField) {
			this.languageField = languageField;
			return this;
		}

		public Builder<K, V> defaultScore(Double defaultScore) {
			this.defaultScore = defaultScore;
			return this;
		}

		public Builder<K, V> scoreField(K scoreField) {
			this.scoreField = scoreField;
			return this;
		}

		public Builder<K, V> payloadField(K payloadField) {
			this.payloadField = payloadField;
			return this;
		}

		public Builder<K, V> maxTextFields(boolean maxTextFields) {
			this.maxTextFields = maxTextFields;
			return this;
		}

		public Builder<K, V> temporary(Long temporary) {
			this.temporary = temporary;
			return this;
		}

		public Builder<K, V> noOffsets(boolean noOffsets) {
			this.noOffsets = noOffsets;
			return this;
		}

		public Builder<K, V> noHL(boolean noHL) {
			this.noHL = noHL;
			return this;
		}

		public Builder<K, V> noFields(boolean noFields) {
			this.noFields = noFields;
			return this;
		}

		public Builder<K, V> noFreqs(boolean noFreqs) {
			this.noFreqs = noFreqs;
			return this;
		}

		public Builder<K, V> noItitialScan(boolean noItitialScan) {
			this.noItitialScan = noItitialScan;
			return this;
		}

		public Builder<K, V> stopWords(List<V> stopWords) {
			this.stopWords = stopWords;
			return this;
		}

		public CreateOptions<K, V> build() {
			return new CreateOptions<>(this);
		}
	}

}
