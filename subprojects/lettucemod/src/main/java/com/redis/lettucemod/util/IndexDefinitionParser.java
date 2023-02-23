package com.redis.lettucemod.util;

import java.util.Iterator;
import java.util.List;

import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Language;

public class IndexDefinitionParser {

	private final CreateOptions.Builder<String, String> options;
	private final Iterator<Object> iterator;

	public IndexDefinitionParser(List<Object> list, CreateOptions.Builder<String, String> options) {
		this.iterator = list.iterator();
		this.options = options;
	}

	public CreateOptions<String, String> parse() {
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			if (key.equals("key_type")) {
				options.on(DataType.valueOf(nextString().toUpperCase()));
			} else if (key.equals("prefixes")) {
				options.prefixes(nextStringArray());
			} else if (key.equals("filter")) {
				options.filter(nextString());
			} else if (key.equals("default_language")) {
				options.defaultLanguage(Language.valueOf(nextString().toUpperCase()));
			} else if (key.equals("language_field")) {
				options.languageField(nextString());
			} else if (key.equals("default_score")) {
				options.defaultScore(nextDouble());
			} else if (key.equals("score_field")) {
				options.scoreField(nextString());
			} else if (key.equals("payload_field")) {
				options.payloadField(nextString());
			}
		}
		return options.build();

	}

	private double nextDouble() {
		return RedisModulesUtils.getDouble(iterator.next());
	}

	private String nextString() {
		return (String) iterator.next();
	}

	@SuppressWarnings("unchecked")
	private String[] nextStringArray() {
		return ((List<Object>) iterator.next()).toArray(new String[0]);
	}

}