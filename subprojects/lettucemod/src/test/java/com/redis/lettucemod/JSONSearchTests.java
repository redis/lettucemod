package com.redis.lettucemod;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.testcontainers.RedisModulesContainer;
import lombok.Data;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class JSONSearchTests {

    @Data
    private static class Beer {
        private String id;
        private String name;
        private Style style;
    }

    @Data
    private static class Style {
        private long id;
        private String name;
    }

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer("preview");

    @Test
    void testJSONSearch() throws Exception {
        JsonItemReaderBuilder<Beer> jsonReaderBuilder = new JsonItemReaderBuilder<>();
        jsonReaderBuilder.name("beer-json-reader");
        jsonReaderBuilder.resource(new ClassPathResource("beers.json"));
        JacksonJsonObjectReader<Beer> jsonObjectReader = new JacksonJsonObjectReader<>(Beer.class);
        jsonObjectReader.setMapper(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false));
        jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
        JsonItemReader<Beer> reader = jsonReaderBuilder.build();
        reader.open(new ExecutionContext());
        ObjectWriter jsonWriter = new ObjectMapper().writer();
        RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
        StatefulRedisModulesConnection<String, String> connection = client.connect();
        RediSearchCommands<String,String> redisearch = connection.sync();
        String index = "beers";
        redisearch.create(index, CreateOptions.<String, String>builder().on(CreateOptions.DataType.JSON).build(), Field.tag("$.id").as("id").build(), Field.text("$.name").as("name").build(), Field.numeric("$.style.id").as("styleId").build(), Field.text("$.style.name").as("styleName").build());
        RedisJSONCommands<String, String> redisjson = connection.sync();
        Beer beer;
        while ((beer = reader.read()) != null) {
            redisjson.set("beer:" + beer.getId(), "$", jsonWriter.writeValueAsString(beer));
        }
        SearchResults<String, String> results = redisearch.search(index, "@name:California");
        Assertions.assertEquals(1, results.getCount());
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
    }
}
