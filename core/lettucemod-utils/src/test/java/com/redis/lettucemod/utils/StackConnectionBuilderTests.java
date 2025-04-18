package com.redis.lettucemod.utils;

import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

public class StackConnectionBuilderTests extends AbstractConnectionBuilderTests {

    private DockerImageName imageName = RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG);

    private final RedisStackContainer container = new RedisStackContainer(imageName);

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

}
