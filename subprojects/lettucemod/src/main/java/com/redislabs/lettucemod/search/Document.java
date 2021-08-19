package com.redislabs.lettucemod.search;

import io.lettuce.core.internal.LettuceAssert;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class Document<K, V> extends LinkedHashMap<K, V> {

    private K id;
    private Double score;
    private V sortKey;
    private V payload;

    public static <K, V> DocumentBuilder<K, V> builder(K id) {
        return new DocumentBuilder<K, V>().id(id);
    }

    public static <K, V> DocumentBuilder<K, V> builder() {
        return new DocumentBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class DocumentBuilder<K, V> {

        private K id;
        private double score = 1;
        private V payload;
        private Map<K, V> fields = new HashMap<>();

        public DocumentBuilder<K, V> field(K name, V value) {
            fields.put(name, value);
            return this;
        }

        public Document<K, V> build() {
            LettuceAssert.notNull(id, "Id is required.");
            LettuceAssert.notNull(fields, "Fields are required.");
            Document<K, V> document = new Document<>();
            document.setId(id);
            document.setScore(score);
            document.setPayload(payload);
            document.putAll(fields);
            return document;
        }

    }

}
