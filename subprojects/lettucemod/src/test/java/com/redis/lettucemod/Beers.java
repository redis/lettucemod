package com.redis.lettucemod;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.TextField.PhoneticMatcher;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;

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

    private static final Field<String> FIELD_ID = Field.tag(ID).sortable().build();

    private static final Field<String> FIELD_BREWERY = Field.tag(BREWERY).sortable().build();

    private static final Field<String> FIELD_NAME = Field.text(NAME).sortable().build();

    private static final Field<String> FIELD_ABV = Field.numeric(ABV).sortable().build();

    private static final Field<String> FIELD_IBU = Field.numeric(IBU).sortable().build();

    private static final Field<String> FIELD_DESCRIPTION = Field.text(DESCRIPTION).matcher(PhoneticMatcher.ENGLISH).noStem()
            .build();

    private static final Field<String> FIELD_STYLE_NAME = Field.tag(STYLE).sortable().build();

    private static final Field<String> FIELD_CATEGORY_NAME = Field.tag(CATEGORY).sortable().build();

    @SuppressWarnings("unchecked")
    private static final Field<String>[] SCHEMA = new Field[] { FIELD_ID, FIELD_NAME, FIELD_STYLE_NAME, FIELD_CATEGORY_NAME,
            FIELD_BREWERY, FIELD_DESCRIPTION, FIELD_ABV, FIELD_IBU };

    private static final String file = "beers.json";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void createIndex(StatefulRedisModulesConnection<String, String> connection) {
        CreateOptions<String, String> options = CreateOptions.<String, String> builder().prefix(PREFIX).payloadField(PAYLOAD)
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
        RedisModulesAsyncCommands<String, String> async = connection.async();
        List<RedisFuture<?>> futures = new ArrayList<>();
        try {
            MappingIterator<Map<String, Object>> iterator = mapIterator();
            while (iterator.hasNext()) {
                Map<String, Object> beer = iterator.next();
                beer.put(PAYLOAD, beer.get(DESCRIPTION));
                futures.add(async.hset(PREFIX + beer.get(ID), (Map) beer));
            }
            connection.flushCommands();
            LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
        } finally {
            connection.setAutoFlushCommands(true);
        }
        return futures.size();
    }

}
