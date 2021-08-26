package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.aggregate.Filter;
import com.redis.lettucemod.search.aggregate.GroupBy;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
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

        // RedisJSON
        RedisJSONCommands<String, String> json = connection.sync(); // <1>
        json.set("arr", ".", "[1,2,3]"); // <2>

    }

    public void connectionPooling() {
        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379");
        GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(16);
        GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, config);
        try (StatefulRedisModulesConnection<String,String> connection = pool.borrowObject()) {
            RedisModulesAsyncCommands<String, String> commands = connection.async();
            // ...
        } catch (Exception e) {
            log.error("Could not get a connection from the pool", e);
        }
    }

    @Data
    private static class MyEntity {
        private final String name;
        private final double score;
    }

    private Collection<MyEntity> entities() {
        return Collections.singletonList(new MyEntity("Hancock", 1));
    }

    public void pipelining() {
        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379");
        StatefulRedisModulesConnection<String, String> connection = client.connect();
        RedisModulesAsyncCommands<String, String> commands = connection.async();

        commands.setAutoFlushCommands(false); // <1>

        List<RedisFuture<?>> futures = new ArrayList<>(); // <2>
        for (MyEntity element : entities()) {
            futures.add(commands.sugadd("names", element.getName(), element.getScore()));
        }

        commands.flushCommands(); // <3>

        boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
                futures.toArray(new RedisFuture[0])); // <4>

        connection.close(); // <5>
    }

    public void warnings() {
        AggregateOptions.AggregateOptionsBuilder<String, String> optionsBuilder = AggregateOptions.operation(new Filter<String, String>("foo")).operation(GroupBy.<String, String>property("bar").build());
    }

    public void cluster() {
        RedisURI node1 = RedisURI.create("node1", 6379);
        RedisURI node2 = RedisURI.create("node2", 6379);

        RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(Arrays.asList(node1, node2));
        StatefulRedisModulesClusterConnection<String, String> connection = clusterClient.connect();
        RedisModulesAdvancedClusterCommands<String, String> syncCommands = connection.sync();

    }

}
