package com.redis.lettucemod.search;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RediSearchUtils {

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
            CommandKeyword type = CommandKeyword.valueOf((String) info.get(2));
            boolean sortable = false;
            boolean noIndex = false;
            for (Object attribute : info.subList(3, info.size())) {
                if (CommandKeyword.NOINDEX.name().equals(attribute)) {
                    noIndex = true;
                    continue;
                }
                if (CommandKeyword.SORTABLE.name().equals(attribute)) {
                    sortable = true;
                }
            }
            fields.add(field(type, name, sortable, noIndex, info));
        }
        return fields;
    }

    private static Field field(CommandKeyword type, String name, boolean sortable, boolean noIndex, List<Object> info) {
        switch (type) {
            case GEO:
                return new Field.Geo(name, sortable, noIndex);
            case NUMERIC:
                return new Field.Numeric(name, sortable, noIndex);
            case TAG:
                return new Field.Tag(name, sortable, noIndex, (String) info.get(4));
            default:
                return new Field.Text(name, sortable, noIndex, getDouble(info.get(4)), CommandKeyword.NOSTEM.name().equals(info.get(info.size() - 1)));
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


}
