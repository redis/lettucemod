package com.redislabs.springredismodules;

import com.redislabs.mesclun.RedisModulesClient;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.search.Suggestion;
import com.redislabs.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RedisModulesAutoConfiguration}.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisModulesAutoConfigurationIntegrationTests {

    @Container
    static final RedisModulesContainer redisModules = new RedisModulesContainer();

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(RedisModulesAutoConfiguration.class)).withPropertyValues("spring.redis.host: " + redisModules.getHost(), "spring.redis.port:" + redisModules.getFirstMappedPort());

    @Test
    void defaultConfiguration() {
        this.contextRunner.run((context) -> {
            assertThat(context.getBean("client")).isInstanceOf(RedisModulesClient.class);
            assertThat(context).hasSingleBean(RedisModulesClient.class);
            assertThat(context).hasSingleBean(StatefulRedisModulesConnection.class);
            RedisModulesClient client = context.getBean(RedisModulesClient.class);
            StatefulRedisModulesConnection<String, String> connection = client.connect();
            String key = "suggestIdx";
            connection.sync().sugadd(key, "rome", 1);
            connection.sync().sugadd(key, "romarin", 1);
            List<Suggestion<String>> suggestions = connection.sync().sugget(key, "rom");
            Assertions.assertEquals(2, suggestions.size());
        });
    }

}