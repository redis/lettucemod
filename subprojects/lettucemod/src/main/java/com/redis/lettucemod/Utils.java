package com.redis.lettucemod;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.IndexInfo;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {

    private final static Long ZERO = 0L;
    private static final String GEO_LONLAT_SEPARATOR = ",";

    @SuppressWarnings("unchecked")
    public static IndexInfo indexInfo(List<Object> infoList) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < (infoList.size() / 2); i++) {
            map.put((String) infoList.get(i * 2), infoList.get(i * 2 + 1));
        }
        return IndexInfo.builder().indexName(getString(map.get("index_name"))).indexOptions((List<Object>) map.get("index_options")).fields(fields(map.get("fields"))).numDocs(getDouble(map.get("num_docs"))).maxDocId(getString(map.get("max_doc_id"))).numTerms(toLong(map, "num_terms")).numRecords(toLong(map, "num_records")).invertedSizeMb(getDouble(map.get("inverted_sz_mb"))).totalInvertedIndexBlocks(toLong(map, "total_inverted_index_blocks")).offsetVectorsSizeMb(getDouble(map.get("offset_vectors_sz_mb"))).docTableSizeMb(getDouble(map.get("doc_table_size_mb"))).sortableValuesSizeMb(getDouble(map.get("sortable_values_size_mb"))).keyTableSizeMb(getDouble(map.get("key_table_size_mb"))).recordsPerDocAvg(getDouble(map.get("records_per_doc_avg"))).bytesPerRecordAvg(getDouble(map.get("bytes_per_record_avg"))).offsetsPerTermAvg(getDouble(map.get("offsets_per_term_avg"))).offsetBitsPerRecordAvg(getDouble(map.get("offset_bits_per_record_avg"))).gcStats((List<Object>) map.get("gc_stats")).cursorStats((List<Object>) map.get("cursor_stats")).build();
    }

    private static Double getDouble(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return LettuceStrings.toDouble((String) object);
        }
        if (object instanceof Long) {
            return ((Long) object).doubleValue();
        }
        return (Double) object;
    }

    private static String getString(Object object) {
        if (object != null) {
            if (object instanceof String) {
                return (String) object;
            }
            if (ZERO.equals(object)) {
                return null;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static List<Field> fields(Object object) {
        List<Field> fields = new ArrayList<>();
        for (Object infoObject : (List<Object>) object) {
            List<Object> info = (List<Object>) infoObject;
            String name = (String) info.get(0);
            SearchCommandKeyword type = SearchCommandKeyword.valueOf((String) info.get(2));
            Field.Options.OptionsBuilder options = Field.Options.builder();
            for (Object attribute : info.subList(3, info.size())) {
                if (SearchCommandKeyword.CASESENSITIVE.name().equals(attribute)) {
                    options.caseSensitive(true);
                    continue;
                }
                if (SearchCommandKeyword.NOINDEX.name().equals(attribute)) {
                    options.noIndex(true);
                    continue;
                }
                if (SearchCommandKeyword.SORTABLE.name().equals(attribute)) {
                    options.sortable(true);
                }
                if (SearchCommandKeyword.UNF.name().equals(attribute)) {
                    options.unNormalizedForm(true);
                }
            }
            fields.add(field(type, name, options.build(), info));
        }
        return fields;
    }

    private static Field field(SearchCommandKeyword type, String name, Field.Options options, List<Object> info) {
        switch (type) {
            case GEO:
                return new Field.Geo(name, options);
            case NUMERIC:
                return new Field.Numeric(name, options);
            case TAG:
                return new Field.Tag(name, options, (String) info.get(4));
            default:
                return new Field.Text(name, options, getDouble(info.get(4)), SearchCommandKeyword.NOSTEM.name().equals(info.get(info.size() - 1)));
        }
    }

    private static Long toLong(Map<String, Object> map, String key) {
        if (map.containsKey(key)) {
            Object value = map.get(key);
            if (value != null) {
                if (value instanceof Long) {
                    return (Long) value;
                }
                if (value instanceof String) {
                    String string = (String) value;
                    if (string.length() > 0) {
                        try {
                            return Long.parseLong(string);
                        } catch (NumberFormatException e) {
                            // ignore
                        }
                    }
                }
            }
        }
        return null;
    }

    public static String escapeTag(String value) {
        return value.replaceAll("([^a-zA-Z0-9])", "\\\\$1");
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GeoLocation {

        private double latitude;
        private double longitude;

        public static GeoLocation of(String location) {
            LettuceAssert.notNull(location, "Location string must not be null");
            String[] lonlat = location.split(GEO_LONLAT_SEPARATOR);
            LettuceAssert.isTrue(lonlat.length == 2, "Location string not in proper format \"longitude,latitude\"");
            return GeoLocation.builder().longitude(Double.parseDouble(lonlat[0])).latitude(Double.parseDouble(lonlat[1])).build();
        }

        public static String toString(String longitude, String latitude) {
            if (longitude == null || latitude == null) {
                return null;
            }
            return longitude + GEO_LONLAT_SEPARATOR + latitude;
        }

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


}
