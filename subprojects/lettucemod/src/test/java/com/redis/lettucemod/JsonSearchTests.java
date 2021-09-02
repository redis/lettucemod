package com.redis.lettucemod;

import com.redis.testcontainers.RedisModulesContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class JsonSearchTests {

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer("preview");

//    @Test
//    void testJsonSearch() throws Exception {
//        JsonItemReader<JsonNode> reader = Beers.jsonReader();
//        RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
//        StatefulRedisModulesConnection<String, String> connection = client.connect();
//        RediSearchCommands<String,String> redisearch = connection.sync();
//        String index = "beers";
//        redisearch.create(index, CreateOptions.<String, String>builder().on(CreateOptions.DataType.JSON).build(), Field.tag("$.id").as("id").build(), Field.text("$.name").as("name").build(), Field.numeric("$.style.id").as("styleId").build(), Field.text("$.style.name").as("styleName").build());
//        RedisJSONCommands<String, String> redisjson = connection.sync();
//        JsonNode beer;
//        while ((beer = reader.read()) != null) {
//            redisjson.set("beer:" + beer.get("id").asText(), "$", beer.toString());
//        }
//        SearchResults<String, String> results = redisearch.search(index, "@name:California");
//        Assertions.assertEquals(1, results.getCount());
//        connection.close();
//        client.shutdown();
//        client.getResources().shutdown();
//    }

}
