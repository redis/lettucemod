package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsCommands;
import com.redislabs.mesclun.search.Field;
import com.redislabs.mesclun.search.RediSearchCommands;
import com.redislabs.mesclun.search.SearchResults;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommands;

@SuppressWarnings({"unchecked", "unused"})
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

    }

}
