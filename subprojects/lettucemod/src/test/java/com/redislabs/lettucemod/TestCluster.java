package com.redislabs.lettucemod;

import com.redislabs.lettucemod.cluster.RedisModulesClusterClient;
import com.redislabs.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redislabs.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import com.redislabs.lettucemod.search.IndexInfo;
import com.redislabs.lettucemod.search.RediSearchUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class TestCluster {

    public static void main(String[] args) {
        RedisModulesClusterClient client = RedisModulesClusterClient.create("redis://redis-12000.jrx.demo.redislabs.com:12000");
        StatefulRedisModulesClusterConnection<String, String> connection = client.connect();
        RedisModulesAdvancedClusterCommands<String, String> sync = connection.sync();
        List<String> keys = sync.keys("*");
        log.info("Keys *: {}", keys.size());
        IndexInfo info = RediSearchUtils.indexInfo(sync.indexInfo("beers"));
        log.info("Num docs: {}", info.getNumDocs());
    }
}
