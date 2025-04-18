package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Field.Type;

import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;

class IndexInfoParser {

    private static final String FIELD_FIELDS = "fields";

    private static final String FIELD_ATTRIBUTES = "attributes";

    public static final String ERROR_UNKNOWN_INDEX_NAME = "Unknown Index name";

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
    private List<Field<String>> fieldsFromAttributes(List<Object> list) {
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

    private Field<String> field(String type, String name) {
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
        if (type.toUpperCase().equals(SearchCommandKeyword.VECTOR.name())) {
            return Field.vector(name).build();
        }
        throw new IllegalArgumentException("Unknown field type: " + type);
    }

    private void populateField(Field<String> field, List<Object> attributes) {
        // TODO Missing from FT.INFO: PHONETIC UNF CASESENSITIVE WITHSUFFIXTRIE
        if (field.getType() == Type.TAG) {
            LettuceAssert.isTrue(SearchCommandKeyword.SEPARATOR.name().equals(attributes.remove(0)), "Wrong attribute name");
            TagField<String> tagField = (TagField<String>) field;
            String separator = (String) attributes.remove(0);
            if (!separator.isEmpty()) {
                tagField.setSeparator(separator.charAt(0));
            }
            tagField.setCaseSensitive(attributes.contains(SearchCommandKeyword.CASESENSITIVE.name()));
        } else {
            if (field.getType() == Type.TEXT) {
                LettuceAssert.isTrue(SearchCommandKeyword.WEIGHT.name().equals(attributes.remove(0)), "Wrong attribute name");
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
    private List<Field<String>> fieldsFromFields(List<Object> list) {
        List<Field<String>> fields = new ArrayList<>();
        for (Object infoObject : list) {
            List<Object> info = (List<Object>) infoObject;
            Field<String> field = field((String) info.get(2), (String) info.get(0));
            populateField(field, info.subList(3, info.size()));
            fields.add(field);
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    public IndexInfo indexInfo() {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setIndexName(getString(map.get("index_name")));
        CreateOptions<String, String> options = createOptions();
        indexInfo.setIndexOptions(options);
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

    private void matchOption(String key, SearchCommandKeyword keyword, Consumer<Boolean> setter) {
        if (key.toUpperCase().equals(keyword.name())) {
            setter.accept(true);
        }
    }

    @SuppressWarnings("unchecked")
    private CreateOptions<String, String> createOptions() {
        CreateOptions.Builder<String, String> options = CreateOptions.builder();
        Iterator<Object> indexOptions = ((List<Object>) map.get("index_options")).iterator();
        // TODO Missing from FT.INFO: NOHL SKIPINITIALSCAN STOPWORDS TEMPORARY
        while (indexOptions.hasNext()) {
            String key = (String) indexOptions.next();
            matchOption(key, SearchCommandKeyword.NOOFFSETS, options::noOffsets);
            matchOption(key, SearchCommandKeyword.NOHL, options::noHL);
            matchOption(key, SearchCommandKeyword.NOFIELDS, options::noFields);
            matchOption(key, SearchCommandKeyword.NOFREQS, options::noFreqs);
            matchOption(key, SearchCommandKeyword.MAXTEXTFIELDS, options::maxTextFields);
        }
        Iterator<Object> indexDefinition = ((List<Object>) map.get("index_definition")).iterator();
        while (indexDefinition.hasNext()) {
            String key = (String) indexDefinition.next();
            if (key.equals("key_type")) {
                options.on(DataType.valueOf(((String) indexDefinition.next()).toUpperCase()));
            } else if (key.equals("prefixes")) {
                options.prefixes(((List<Object>) indexDefinition.next()).toArray(new String[0]));
            } else if (key.equals("filter")) {
                options.filter((String) indexDefinition.next());
            } else if (key.equals("default_language")) {
                options.defaultLanguage(Language.valueOf(((String) indexDefinition.next()).toUpperCase()));
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
