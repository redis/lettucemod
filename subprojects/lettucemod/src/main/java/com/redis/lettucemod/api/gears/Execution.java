package com.redis.lettucemod.api.gears;

import lombok.Data;

@Data
public class Execution {

    private String id;
    private String status;
    private long registered;
}
