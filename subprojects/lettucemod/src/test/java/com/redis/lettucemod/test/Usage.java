package com.redis.lettucemod.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.Filter;
import com.redis.lettucemod.search.Group;
import com.redis.lettucemod.search.SearchResults;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.support.ConnectionPoolSupport;

@SuppressWarnings("unused")
public class Usage {

	private static final Logger log = LoggerFactory.getLogger(Usage.class);

	void usage() {

		// @formatter:off


// Standalone Client

RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379"); // <1>

StatefulRedisModulesConnection<String, String> connection = client.connect(); // <2>

RedisModulesCommands<String, String> commands = connection.sync(); // <3>


// RediSearch
		
RediSearchCommands<String, String> search = connection.sync(); // <1>

search.create("beers", Field.text("name").build(), Field.numeric("ibu").build()); // <2>

SearchResults<String, String> results = search.search("beers", "chou*"); // <3>


// RedisGears

RedisGearsCommands<String, String> gears = connection.sync(); // <1>

gears.pyexecute("GearsBuilder().run('person:*')"); // <2>


// RedisJSON

RedisJSONCommands<String, String> json = connection.sync(); // <1>

json.jsonSet("arr", ".", "[1,2,3]"); // <2>


// RedisTimeSeries

RedisTimeSeriesCommands<String, String> ts = connection.sync(); // <1>

ts.add("temp:3:11", 1548149181, 30); // <2>


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

	public void pipelining() {
		RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379");
		StatefulRedisModulesConnection<String, String> connection = client.connect();

		// @formatter:off
		
// Pipelining
		
RedisModulesAsyncCommands<String, String> commands = connection.async();

commands.setAutoFlushCommands(false); // <1>

List<RedisFuture<?>> futures = new ArrayList<>(); // <2>
for (MyEntity element : entities()) {
	futures.add(commands.sugadd("names", element.getName(), element.getScore()));
}

commands.flushCommands(); // <3>

boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0])); // <4>

connection.close(); // <5>

		// @formatter:on

	}

	public void connectionPooling() {
		RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379");

		// @formatter:off
		
// Connection Pooling

GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> config = new GenericObjectPoolConfig<>(); // <1>

config.setMaxTotal(16);

// ...

GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, config); // <2>

try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) { // <3>
	
	RedisModulesAsyncCommands<String, String> commands = connection.async(); // <4>

	// ...

} catch (Exception e) {
	
	log.error("Could not get a connection from the pool", e);
	
}

        // @formatter:on

	}

	public void warnings() {
		AggregateOptions.Builder<String, String> optionsBuilder = AggregateOptions
				.<String, String>filter(new Filter<>("foo")).group(Group.by("bar").build());
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
