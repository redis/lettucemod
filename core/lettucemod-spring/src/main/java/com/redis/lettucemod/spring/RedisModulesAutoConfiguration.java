package com.redis.lettucemod.spring;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Lettuce.Cluster.Refresh;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.utils.URIBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RedisProperties.class)
public class RedisModulesAutoConfiguration {

    @Bean
    RedisURI redisURI(RedisProperties properties) {
        URIBuilder builder = new URIBuilder();
        if (StringUtils.hasLength(properties.getUrl())) {
            builder.uri(RedisURI.create(properties.getUrl()));
        }
        builder.host(properties.getHost());
        builder.port(properties.getPort());
        builder.clientName(properties.getClientName());
        builder.database(properties.getDatabase());
        builder.username(properties.getUsername());
        if (StringUtils.hasLength(properties.getPassword())) {
            builder.password(properties.getPassword());
        }
        if (properties.getSsl() != null) {
            builder.tls(properties.getSsl().isEnabled());
        }
        builder.timeout(properties.getTimeout());
        return builder.build();
    }

    private <B extends ClientOptions.Builder> B clientOptions(B builder, RedisProperties properties) {
        RedisModulesClient.defaultClientOptions(builder);
        Duration connectTimeout = properties.getConnectTimeout();
        if (connectTimeout != null) {
            builder.socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build());
        }
        builder.timeoutOptions(TimeoutOptions.enabled());
        return builder;
    }

    @Bean(destroyMethod = "shutdown")
    ClientResources clientResources() {
        return DefaultClientResources.create();
    }

    @Bean(destroyMethod = "shutdown", name = "redisModulesClusterClient")
    @ConditionalOnProperty(name = "spring.data.redis.cluster.nodes[0]")
    RedisModulesClusterClient clusterClient(RedisURI redisURI, RedisProperties properties, ClientResources clientResources) {
        RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources, redisURI);
        ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
        Refresh refreshProperties = properties.getLettuce().getCluster().getRefresh();
        ClusterTopologyRefreshOptions.Builder refreshBuilder = ClusterTopologyRefreshOptions.builder()
                .dynamicRefreshSources(refreshProperties.isDynamicRefreshSources());
        if (refreshProperties.getPeriod() != null) {
            refreshBuilder.enablePeriodicRefresh(refreshProperties.getPeriod());
        }
        if (refreshProperties.isAdaptive()) {
            refreshBuilder.enableAllAdaptiveRefreshTriggers();
        }
        builder.topologyRefreshOptions(refreshBuilder.build());
        client.setOptions(clientOptions(builder, properties).build());
        return client;
    }

    @Bean(destroyMethod = "shutdown", name = "redisModulesClient")
    @ConditionalOnMissingBean(name = "redisModulesClusterClient")
    RedisModulesClient redisModulesClient(RedisURI redisURI, RedisProperties properties, ClientResources clientResources) {
        RedisModulesClient client = RedisModulesClient.create(clientResources, redisURI);
        client.setOptions(clientOptions(ClientOptions.builder(), properties).build());
        return client;
    }

    private <K, V, C extends StatefulRedisModulesConnection<K, V>> GenericObjectPoolConfig<C> poolConfig(
            RedisProperties redisProperties) {
        GenericObjectPoolConfig<C> config = new GenericObjectPoolConfig<>();
        config.setJmxEnabled(false);
        Pool pool = redisProperties.getLettuce().getPool();
        if (pool != null) {
            config.setMaxTotal(pool.getMaxActive());
            config.setMaxIdle(pool.getMaxIdle());
            config.setMinIdle(pool.getMinIdle());
            if (pool.getMaxWait() != null) {
                config.setMaxWait(pool.getMaxWait());
            }
            if (pool.getTimeBetweenEvictionRuns() != null) {
                config.setTimeBetweenEvictionRuns(pool.getTimeBetweenEvictionRuns());
            }
        }
        return config;
    }

    @Bean(name = "redisConnectionPool", destroyMethod = "close")
    @ConditionalOnBean(AbstractRedisClient.class)
    GenericObjectPool<StatefulRedisModulesConnection<String, String>> redisConnectionPool(RedisProperties properties,
            AbstractRedisClient client) {
        GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = poolConfig(properties);
        if (client instanceof RedisModulesClusterClient) {
            RedisModulesClusterClient clusterClient = (RedisModulesClusterClient) client;
            return ConnectionPoolSupport.createGenericObjectPool(clusterClient::connect, poolConfig);
        }
        RedisModulesClient redisClient = (RedisModulesClient) client;
        return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
    }

}
