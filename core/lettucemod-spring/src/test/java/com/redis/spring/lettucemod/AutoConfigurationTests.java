package com.redis.spring.lettucemod;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.RedisModulesConnectionBuilder;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.spring.RedisModulesAutoConfiguration;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.resource.ClientResources;

/**
 * Integration tests for {@link RedisModulesAutoConfiguration}.
 */
@Testcontainers
class AutoConfigurationTests {

    @Container
    static final RedisStackContainer redisStackContainer = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RedisModulesAutoConfiguration.class))
            .withPropertyValues("spring.data.redis.host: " + redisStackContainer.getHost(),
                    "spring.data.redis.port:" + redisStackContainer.getFirstMappedPort(),
                    "spring.data.redis.ssl.enabled:false");

    @Test
    void defaultConfiguration() {
        this.contextRunner.run((context) -> {
            assertThat(context).hasSingleBean(AbstractRedisClient.class);
            if (context.containsBean("redisModulesClient")) {
                assertThat(context.getBean("redisModulesClient")).isInstanceOf(RedisModulesClient.class);
            }
            if (context.containsBean("redisModulesClusterClient")) {
                assertThat(context.getBean("redisModulesClusterClient")).isInstanceOf(RedisModulesClusterClient.class);
            }
            assertThat(context).hasSingleBean(ClientResources.class);
            AbstractRedisClient client = context.getBean(AbstractRedisClient.class);
            StatefulRedisModulesConnection<String, String> connection = RedisModulesConnectionBuilder.client(client)
                    .connection();
            String key = "suggestIdx";
            connection.sync().ftSugadd(key, Suggestion.of("rome", 1));
            connection.sync().ftSugadd(key, Suggestion.of("romarin", 1));
            List<Suggestion<String>> suggestions = connection.sync().ftSugget(key, "rom");
            Assertions.assertEquals(2, suggestions.size());
        });
    }

}
