package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.search.arguments.*;

class IndexInfoParser {

    private static final String FIELD_FIELDS = "fields";

    private static final String FIELD_ATTRIBUTES = "attributes";

    public static final String ERROR_UNKNOWN_INDEX_NAME = "Unknown Index name";

    private static final String GEO = new GeoFieldArgs.Builder<String>().build().getFieldType();

    private static final String GEOSHAPE = new GeoshapeFieldArgs.Builder<String>().build().getFieldType();

    private static final String NUMERIC = new NumericFieldArgs.Builder<String>().build().getFieldType();

    private static final String TAG = new TagFieldArgs.Builder<String>().build().getFieldType();

    private static final String TEXT = new TextFieldArgs.Builder<String>().build().getFieldType();

    private static final String VECTOR = new VectorFieldArgs.Builder<String>().build().getFieldType();

    private final Map<String, Object> map;

    public IndexInfoParser(List<Object> list) {
        this.map = toMap(list);
    }

    public static Map<String, Object> toMap(List<Object> list) {
        LettuceAssert.isTrue(list.size() % 2 == 0,
                "List must be a multiple of 2 and contain a sequence of field1, value1, field2, value2, ..., fieldN, valueN");
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            map.put((String) list.get(i), list.get(i + 1));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private List<FieldArgs<String>> fieldsFromAttributes(List<Object> list) {
        List<FieldArgs<String>> fields = new ArrayList<>();
        for (Object object : list) {
            List<Object> attributes = (List<Object>) object;
            String fieldType = (String) attributes.get(5);
            FieldArgs.Builder<String, ?, ?> field = field(fieldType);
            field.name((String) attributes.get(1));
            field.as((String) attributes.get(3));
            if (attributes.size() > 6) {
                apply(field, attributes.subList(6, attributes.size()));
            }
            fields.add(field.build());
        }
        return fields;
    }

    private String getString(Object object) {
        if (object instanceof String) {
            return (String) object;
        }
        return null;
    }

    private Long toLong(Map<String, Object> map, String key) {
        if (!map.containsKey(key)) {
            return null;
        }
        return getLong(map.get(key));
    }

    private FieldArgs.Builder<String, ?, ?> field(String type) {
        switch (type.toUpperCase()) {
            case "GEO":
                return new GeoFieldArgs.Builder<>();
            case "GEOSHAPE":
                return new GeoshapeFieldArgs.Builder<>();
            case "NUMERIC":
                return new NumericFieldArgs.Builder<>();
            case "TAG":
                return new TagFieldArgs.Builder<>();
            case "TEXT":
                return new TextFieldArgs.Builder<>();
            case "VECTOR":
                return new VectorFieldArgs.Builder<>();
            default:
                throw new IllegalArgumentException("Unknown field type: " + type);
        }
    }

    private void apply(FieldArgs.Builder<String, ?, ?> field, List<Object> attributes) {
        // TODO Missing from FT.INFO: PHONETIC UNF CASESENSITIVE WITHSUFFIXTRIE
        if (field instanceof TagFieldArgs.Builder) {
            LettuceAssert.isTrue(CommandKeyword.SEPARATOR.name().equals(attributes.remove(0)), "Wrong attribute name");
            TagFieldArgs.Builder<String> tagField = (TagFieldArgs.Builder<String>) field;
            String separator = (String) attributes.remove(0);
            if (!separator.isEmpty()) {
                tagField.separator(separator);
            }
            if (attributes.contains(CommandKeyword.CASESENSITIVE.name())) {
                tagField.caseSensitive();
            }
        }
        if (field instanceof TextFieldArgs.Builder) {
            LettuceAssert.isTrue(CommandKeyword.WEIGHT.name().equals(attributes.remove(0)), "Wrong attribute name");
            TextFieldArgs.Builder<String> textField = (TextFieldArgs.Builder<String>) field;
            Object weight = attributes.remove(0);
            textField.weight(getLong(weight));
            if (attributes.contains(CommandKeyword.NOSTEM.name())) {
                textField.noStem();
            }
        }
        if (attributes.contains(CommandKeyword.NOINDEX.name())) {
            field.noIndex();
        }
        if (attributes.contains(CommandKeyword.SORTABLE.name())) {
            field.sortable();
        }
        if (attributes.contains(CommandKeyword.UNF.name())) {
            field.unNormalizedForm();
        }
    }

    @SuppressWarnings("unchecked")
    private List<FieldArgs<String>> fieldsFromFields(List<Object> list) {
        List<FieldArgs<String>> fields = new ArrayList<>();
        for (Object infoObject : list) {
            List<Object> info = (List<Object>) infoObject;
            String fieldName = (String) info.get(0);
            String fieldType = (String) info.get(2);
            FieldArgs.Builder<String, ?, ?> field = field(fieldType);
            field.name(fieldName);
            apply(field, info.subList(3, info.size()));
            fields.add(field.build());
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    public IndexInfo indexInfo() {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setIndexName(getString(map.get("index_name")));
        CreateArgs<String, String> options = createOptions();
        indexInfo.setIndexArgs(options);
        if (map.containsKey(FIELD_FIELDS)) {
            indexInfo.setFields(fieldsFromFields((List<Object>) map.getOrDefault(FIELD_FIELDS, new ArrayList<>())));
        }
        if (map.containsKey(FIELD_ATTRIBUTES)) {
            indexInfo.setFields(fieldsFromAttributes((List<Object>) map.getOrDefault(FIELD_ATTRIBUTES, new ArrayList<>())));
        }
        indexInfo.setNumDocs(getDouble(map.get("num_docs")));
        indexInfo.setMaxDocId(getString(map.get("max_doc_id")));
        indexInfo.setNumTerms(toLong(map, "num_terms"));
        indexInfo.setNumRecords(toLong(map, "num_records"));
        indexInfo.setInvertedSizeMb(getDouble(map.get("inverted_sz_mb")));
        indexInfo.setTotalInvertedIndexBlocks(toLong(map, "total_inverted_index_blocks"));
        indexInfo.setVectorIndexSizeMb(getDouble(map.get("vector_index_sz_mb")));
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

    private boolean matchOption(String key, CommandKeyword keyword) {
        return key.toUpperCase().equals(keyword.name());
    }

    @SuppressWarnings("unchecked")
    private CreateArgs<String, String> createOptions() {
        CreateArgs.Builder<String, String> options = CreateArgs.builder();
        Iterator<Object> indexOptions = ((List<Object>) map.get("index_options")).iterator();
        // TODO Missing from FT.INFO: NOHL SKIPINITIALSCAN STOPWORDS TEMPORARY
        while (indexOptions.hasNext()) {
            String key = (String) indexOptions.next();
            if (matchOption(key, CommandKeyword.NOOFFSETS)) {
                options.noOffsets();
            }
            if (matchOption(key, CommandKeyword.NOHL)) {
                options.noHighlighting();
            }
            if (matchOption(key, CommandKeyword.NOFIELDS)) {
                options.noOffsets();
            }
            if (matchOption(key, CommandKeyword.NOFREQS)) {
                options.noFrequency();
            }
            if (matchOption(key, CommandKeyword.MAXTEXTFIELDS)) {
                options.maxTextFields();
            }
        }
        Iterator<Object> indexDefinition = ((List<Object>) map.get("index_definition")).iterator();
        while (indexDefinition.hasNext()) {
            String key = (String) indexDefinition.next();
            if (key.equals("key_type")) {
                options.on(CreateArgs.TargetType.valueOf(((String) indexDefinition.next()).toUpperCase()));
            } else if (key.equals("prefixes")) {
                options.withPrefixes((List<String>) indexDefinition.next());
            } else if (key.equals("filter")) {
                options.filter((String) indexDefinition.next());
            } else if (key.equals("default_language")) {
                options.defaultLanguage(DocumentLanguage.valueOf(((String) indexDefinition.next()).toUpperCase()));
            } else if (key.equals("language_field")) {
                options.languageField((String) indexDefinition.next());
            } else if (key.equals("default_score")) {
                options.defaultScore(getDouble(indexDefinition.next()));
            } else if (key.equals("score_field")) {
                options.scoreField((String) indexDefinition.next());
            } else if (key.equals("payload_field")) {
                options.payloadField((String) indexDefinition.next());
            }
        }
        return options.build();
    }

    private Long getLong(Object object) {
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

    public Double getDouble(Object object) {
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

}
