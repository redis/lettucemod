package com.redis.lettucemod;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.FieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;

public class Beers {

    public static final String PREFIX = "beer:";

    public static final String INDEX = "beers";

    public static final String PAYLOAD = "payload";

    public static final String NAME = "name";

    public static final String ABV = "abv";

    public static final String IBU = "ibu";

    public static final String DESCRIPTION = "descript";

    public static final String STYLE = "style_name";

    public static final String CATEGORY = "cat_name";

    public static final String ID = "id";

    public static final String BREWERY = "brewery_id";

    private static final FieldArgs<String> FIELD_ID = TagFieldArgs.<String> builder().name(ID).sortable().build();

    private static final FieldArgs<String> FIELD_BREWERY = TagFieldArgs.<String> builder().name(BREWERY).sortable().build();

    private static final FieldArgs<String> FIELD_NAME = TextFieldArgs.<String> builder().name(NAME).sortable().build();

    private static final FieldArgs<String> FIELD_ABV = NumericFieldArgs.<String> builder().name(ABV).sortable().build();

    private static final FieldArgs<String> FIELD_IBU = NumericFieldArgs.<String> builder().name(IBU).sortable().build();

    private static final FieldArgs<String> FIELD_DESCRIPTION = TextFieldArgs.<String> builder().name(DESCRIPTION)
            .phonetic(TextFieldArgs.PhoneticMatcher.ENGLISH).noStem().build();

    private static final FieldArgs<String> FIELD_STYLE_NAME = TagFieldArgs.<String> builder().name(STYLE).sortable().build();

    private static final FieldArgs<String> FIELD_CATEGORY_NAME = TagFieldArgs.<String> builder().name(CATEGORY).sortable()
            .build();

    private static final List<FieldArgs<String>> SCHEMA = Arrays.asList(FIELD_ID, FIELD_NAME, FIELD_STYLE_NAME,
            FIELD_CATEGORY_NAME, FIELD_BREWERY, FIELD_DESCRIPTION, FIELD_ABV, FIELD_IBU);

    private static final String file = "beers.json";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void createIndex(StatefulRedisModulesConnection<String, String> connection) {
        CreateArgs<String, String> options = CreateArgs.<String, String> builder().withPrefix(PREFIX).payloadField(PAYLOAD)
                .build();
        connection.sync().ftCreate(INDEX, options, SCHEMA);
    }

    public static Iterator<JsonNode> jsonNodeIterator() throws IOException {
        return mapper.readerFor(Map.class).readTree(inputStream()).iterator();
    }

    public static MappingIterator<Map<String, Object>> mapIterator() throws IOException {
        return mapper.readerFor(Map.class).readValues(inputStream());
    }

    private static InputStream inputStream() {
        return Beers.class.getClassLoader().getResourceAsStream(file);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static int populateIndex(StatefulRedisModulesConnection<String, String> connection) throws IOException {
        createIndex(connection);
        connection.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        try {
            MappingIterator<Map<String, Object>> iterator = mapIterator();
            while (iterator.hasNext()) {
                Map<String, Object> beer = iterator.next();
                beer.put(PAYLOAD, beer.get(DESCRIPTION));
                futures.add(connection.async().hset(PREFIX + beer.get(ID), (Map) beer));
            }
            connection.flushCommands();
            LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
        } finally {
            connection.setAutoFlushCommands(true);
        }
        return futures.size();
    }

}
