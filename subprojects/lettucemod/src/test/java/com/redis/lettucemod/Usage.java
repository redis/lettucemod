package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.search.aggregate.Filter;
import com.redis.lettucemod.search.aggregate.GroupBy;

@SuppressWarnings("unused")
public class Usage {

    public void basic() {

        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379"); // <1>
        StatefulRedisModulesConnection<String, String> connection = client.connect(); // <2>

        // RediSearch
        RediSearchCommands<String, String> search = connection.sync(); // <3>
        search.create("beers", Field.text("name").build(), Field.numeric("ibu").build()); // <4>
        SearchResults<String, String> results = search.search("beers", "chou*"); // <5>

        // RedisGears
        RedisGearsCommands<String, String> gears = connection.sync(); // <6>
        gears.pyExecute("GearsBuilder().run('person:*')"); // <7>

        // RedisTimeSeries
        RedisTimeSeriesCommands<String, String> ts = connection.sync(); // <8>
        ts.add("temp:3:11", 1548149181, 30); // <9>

        // RedisJSON
        RedisJSONCommands<String, String> json = connection.sync(); // <1>
        json.set("arr", ".", "[1,2,3]"); // <2>

    }

    public void warnings() {
        AggregateOptions.AggregateOptionsBuilder<String, String> optionsBuilder = AggregateOptions.operation(new Filter<String, String>("foo")).operation(GroupBy.<String, String>property("bar").build());
    }

}
