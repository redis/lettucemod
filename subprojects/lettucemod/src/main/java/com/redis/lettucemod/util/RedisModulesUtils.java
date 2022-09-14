package com.redis.lettucemod.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.Field.Type;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.TagField;
import com.redis.lettucemod.search.TextField;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisModulesUtils {

	private static final String FIELD_FIELDS = "fields";
	private static final String FIELD_ATTRIBUTES = "attributes";

	private RedisModulesUtils() {
	}

	@SuppressWarnings("unchecked")
	public static IndexInfo indexInfo(List<Object> infoList) {
		LettuceAssert.isTrue(infoList.size() % 2 == 0,
				"List must be a multiple of 2 and contain a sequence of field1, value1, field2, value2, ..., fieldN, valueN");
		Map<String, Object> map = new HashMap<>();
		for (int i = 0; i < infoList.size(); i += 2) {
			map.put((String) infoList.get(i), infoList.get(i + 1));
		}
		IndexInfo indexInfo = new IndexInfo();
		indexInfo.setIndexName(getString(map.get("index_name")));
		CreateOptions.Builder<String, String> options = CreateOptions.builder();
		indexOptions((List<Object>) map.get("index_options"), options);
		indexDefinition((List<Object>) map.get("index_definition"), options);
		indexInfo.setIndexOptions(options.build());
		if (map.containsKey(FIELD_FIELDS)) {
			indexInfo.setFields(fieldsFromFields((List<Object>) map.getOrDefault(FIELD_FIELDS, new ArrayList<>())));
		}
		if (map.containsKey(FIELD_ATTRIBUTES)) {
			indexInfo.setFields(
					fieldsFromAttributes((List<Object>) map.getOrDefault(FIELD_ATTRIBUTES, new ArrayList<>())));
		}
		indexInfo.setNumDocs(getDouble(map.get("num_docs")));
		indexInfo.setMaxDocId(getString(map.get("max_doc_id")));
		indexInfo.setNumTerms(toLong(map, "num_terms"));
		indexInfo.setNumRecords(toLong(map, "num_records"));
		indexInfo.setInvertedSizeMb(getDouble(map.get("inverted_sz_mb")));
		indexInfo.setTotalInvertedIndexBlocks(toLong(map, "total_inverted_index_blocks"));
		indexInfo.setOffsetVectorsSizeMb(getDouble(map.get("offset_vectors_sz_mb")));
		indexInfo.setDocTableSizeMb(getDouble(map.get("doc_table_size_mb")));
		indexInfo.setSortableValuesSizeMb(getDouble(map.get("sortable_values_size_mb")));
		indexInfo.setKeyTableSizeMb(getDouble(map.get("key_table_size_mb")));
		indexInfo.setRecordsPerDocAvg(getDouble(map.get("records_per_doc_avg")));
		indexInfo.setBytesPerRecordAvg(getDouble(map.get("bytes_per_record_avg")));
		indexInfo.setOffsetsPerTermAvg(getDouble(map.get("offsets_per_term_avg")));
		indexInfo.setOffsetBitsPerRecordAvg(getDouble(map.get("offset_bits_per_record_avg")));
		indexInfo.setGcStats((List<Object>) map.get("gc_stats"));
		indexInfo.setCursorStats((List<Object>) map.get("cursor_stats"));
		return indexInfo;
	}

	private static void indexOptions(List<Object> list, CreateOptions.Builder<String, String> options) {
		// TODO Missing from FT.INFO: NOHL SKIPINITIALSCAN STOPWORDS TEMPORARY
		Iterator<Object> iterator = list.iterator();
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			matchOption(key, SearchCommandKeyword.NOOFFSETS, options::noOffsets);
			matchOption(key, SearchCommandKeyword.NOHL, options::noHL);
			matchOption(key, SearchCommandKeyword.NOFIELDS, options::noFields);
			matchOption(key, SearchCommandKeyword.NOFREQS, options::noFreqs);
			matchOption(key, SearchCommandKeyword.MAXTEXTFIELDS, options::maxTextFields);
		}
	}

	private static void matchOption(String key, SearchCommandKeyword keyword, Consumer<Boolean> setter) {
		if (key.toUpperCase().equals(keyword.name())) {
			setter.accept(true);
		}
	}

	private static void indexDefinition(List<Object> list, CreateOptions.Builder<String, String> options) {
		new IndexDefinitionParser(list, options).parse();
	}

	private static Long getLong(Object object) {
		if (object instanceof String) {
			try {
				return Long.parseLong((String) object);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if (object instanceof Long) {
			return (Long) object;
		}
		return null;
	}

	static Double getDouble(Object object) {
		if (object instanceof String) {
			return LettuceStrings.toDouble((String) object);
		}
		if (object instanceof Long) {
			return ((Long) object).doubleValue();
		}
		if (object instanceof Double) {
			return (Double) object;
		}
		return null;
	}

	private static String getString(Object object) {
		if (object instanceof String) {
			return (String) object;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static List<Field<String>> fieldsFromAttributes(List<Object> list) {
		List<Field<String>> fields = new ArrayList<>();
		for (Object object : list) {
			List<Object> attributes = (List<Object>) object;
			Field<String> field = field((String) attributes.get(5), (String) attributes.get(1));
			field.setAs((String) attributes.get(3));
			if (attributes.size() > 6) {
				populateField(field, attributes.subList(6, attributes.size()));
			}
			fields.add(field);
		}
		return fields;
	}

	private static void populateField(Field<String> field, List<Object> attributes) {
		// TODO Missing from FT.INFO: PHONETIC UNF CASESENSITIVE WITHSUFFIXTRIE
		if (field.getType() == Type.TAG) {
			LettuceAssert.isTrue(SearchCommandKeyword.SEPARATOR.name().equals(attributes.remove(0)),
					"Wrong attribute name");
			TagField<String> tagField = (TagField<String>) field;
			String separator = (String) attributes.remove(0);
			if (!separator.isEmpty()) {
				tagField.setSeparator(separator.charAt(0));
			}
			tagField.setCaseSensitive(attributes.contains(SearchCommandKeyword.CASESENSITIVE.name()));
		} else {
			if (field.getType() == Type.TEXT) {
				LettuceAssert.isTrue(SearchCommandKeyword.WEIGHT.name().equals(attributes.remove(0)),
						"Wrong attribute name");
				TextField<String> textField = (TextField<String>) field;
				Object weight = attributes.remove(0);
				textField.setWeight(getDouble(weight));
				textField.setNoStem(attributes.contains(SearchCommandKeyword.NOSTEM.name()));
			}
		}
		field.setNoIndex(attributes.contains(SearchCommandKeyword.NOINDEX.name()));
		field.setSortable(attributes.contains(SearchCommandKeyword.SORTABLE.name()));
		field.setUnNormalizedForm(attributes.contains(SearchCommandKeyword.UNF.name()));
	}

	@SuppressWarnings("unchecked")
	private static List<Field<String>> fieldsFromFields(List<Object> list) {
		List<Field<String>> fields = new ArrayList<>();
		for (Object infoObject : list) {
			List<Object> info = (List<Object>) infoObject;
			Field<String> field = field((String) info.get(2), (String) info.get(0));
			populateField(field, info.subList(3, info.size()));
			fields.add(field);
		}
		return fields;
	}

	private static Field<String> field(String type, String name) {
		if (type.toUpperCase().equals(SearchCommandKeyword.GEO.name())) {
			return Field.geo(name).build();
		}
		if (type.toUpperCase().equals(SearchCommandKeyword.NUMERIC.name())) {
			return Field.numeric(name).build();
		}
		if (type.toUpperCase().equals(SearchCommandKeyword.TAG.name())) {
			return Field.tag(name).build();
		}
		if (type.toUpperCase().equals(SearchCommandKeyword.TEXT.name())) {
			return Field.text(name).build();
		}
		throw new IllegalArgumentException("Unknown field type: " + type);
	}

	private static Long toLong(Map<String, Object> map, String key) {
		if (!map.containsKey(key)) {
			return null;
		}
		return getLong(map.get(key));
	}

	public static String escapeTag(String value) {
		return value.replaceAll("([^a-zA-Z0-9])", "\\\\$1");
	}

	public static String toString(InputStream inputStream, Charset charset) {
		return toString(new InputStreamReader(inputStream, charset));
	}

	public static String toString(InputStreamReader reader) {
		return new BufferedReader(reader).lines().collect(Collectors.joining(System.lineSeparator()));
	}

	public static String toString(InputStream inputStream) {
		return toString(new InputStreamReader(inputStream));
	}
	
	public static StatefulRedisModulesConnection<String, String> connection(AbstractRedisClient client) {
		return connection(client, StringCodec.UTF8);
	}

	public static <K, V> StatefulRedisModulesConnection<K, V> connection(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connect(codec);
		}
		return ((RedisModulesClient) client).connect(codec);
	}

	public static StatefulRedisPubSubConnection<String, String> pubSubConnection(AbstractRedisClient client) {
		return pubSubConnection(client, StringCodec.UTF8);
	}

	public static <K, V> StatefulRedisPubSubConnection<K, V> pubSubConnection(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connectPubSub(codec);
		}
		return ((RedisModulesClient) client).connectPubSub(codec);
	}

}
