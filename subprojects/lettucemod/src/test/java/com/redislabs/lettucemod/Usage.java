package com.redislabs.lettucemod;

import com.redislabs.lettucemod.api.StatefulRedisModulesConnection;
import com.redislabs.lettucemod.api.sync.RedisGearsCommands;
import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.Field;
import com.redislabs.lettucemod.api.sync.RediSearchCommands;
import com.redislabs.lettucemod.search.SearchResults;
import com.redislabs.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redislabs.lettucemod.search.aggregate.Filter;
import com.redislabs.lettucemod.search.aggregate.GroupBy;

@SuppressWarnings("unused")
public class Usage {

    @SuppressWarnings("unchecked")
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

    }

    public void warnings() {
        AggregateOptions.AggregateOptionsBuilder<String, String> optionsBuilder = AggregateOptions.operation(new Filter<String, String>("foo")).operation(GroupBy.<String, String>property("bar").build());
    }

}
