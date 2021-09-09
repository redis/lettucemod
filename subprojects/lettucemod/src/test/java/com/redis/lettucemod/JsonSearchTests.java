package com.redis.lettucemod;

import com.fasterxml.jackson.databind.JsonNode;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.SearchResults;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class JsonSearchTests {

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer("preview");

    @Test
    void testJsonSearch() throws Exception {
        JsonItemReader<JsonNode> reader = jsonNodeReader();
        reader.open(new ExecutionContext());
        RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
        StatefulRedisModulesConnection<String, String> connection = client.connect();
        RediSearchCommands<String,String> redisearch = connection.sync();
        String index = "beers";
        redisearch.create(index, CreateOptions.<String, String>builder().on(CreateOptions.DataType.JSON).build(), Field.tag("$.id").as("id").build(), Field.text("$.name").as("name").build(), Field.numeric("$.style.id").as("styleId").build(), Field.text("$.style.name").as("styleName").build());
        RedisJSONCommands<String, String> redisjson = connection.sync();
        JsonNode beer;
        while ((beer = reader.read()) != null) {
            redisjson.set("beer:" + beer.get("id").asText(), "$", beer.toString());
        }
        SearchResults<String, String> results = redisearch.search(index, "@name:California");
        Assertions.assertEquals(1, results.getCount());
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
        reader.close();
    }

    private static JsonItemReader<JsonNode> jsonNodeReader() {
        return new JsonItemReaderBuilder<JsonNode>().jsonObjectReader(new JacksonJsonNodeReader()).name("beer-json-node-reader").resource(new ClassPathResource("beers.json")).build();
    }

}
