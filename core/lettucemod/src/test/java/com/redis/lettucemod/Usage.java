package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.RedisURI;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class Usage {

    private final Logger log = LoggerFactory.getLogger(Usage.class);

    @SuppressWarnings("unchecked")
    void usage() {

        // @formatter:off

        // Standalone Client

        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379"); // <1>

        StatefulRedisModulesConnection<String, String> connection = client.connect(); // <2>

        RedisModulesCommands<String, String> commands = connection.sync(); // <3>

        // RedisTimeSeries

        RedisTimeSeriesCommands<String, String> ts = connection.sync(); // <1>

        ts.tsAdd("temp:3:11", Sample.of(1548149181, 30)); // <2>

        //@formatter:on

    }

    private static class MyEntity {

        private String name;

        private double score;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

    }

    private Collection<MyEntity> entities() {
        MyEntity entity = new MyEntity();
        entity.setName("Hancock");
        entity.setScore(1);
        return Collections.singletonList(entity);
    }

    public void connectionPooling() {
        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379");

        // @formatter:off

        // Connection Pooling

        GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> config = new GenericObjectPoolConfig<>(); // <1>

        config.setMaxTotal(16);

        // ...

        GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(
                client::connect, config); // <2>

        try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) { // <3>

            RedisModulesAsyncCommands<String, String> commands = connection.async(); // <4>

            // ...

        } catch (Exception e) {

            log.error("Could not get a connection from the pool", e);

        }

        // @formatter:on

    }

    public void cluster() {

        // @formatter:off

        // Cluster Client

        List<RedisURI> uris = Arrays.asList(RedisURI.create("node1", 6379), RedisURI.create("node2", 6379)); // <1>

        RedisModulesClusterClient client = RedisModulesClusterClient.create(uris); // <2>

        StatefulRedisModulesClusterConnection<String, String> connection = client.connect(); // <3>

        RedisModulesAdvancedClusterCommands<String, String> commands = connection.sync(); // <4>

        // @formatter:on
    }

}
