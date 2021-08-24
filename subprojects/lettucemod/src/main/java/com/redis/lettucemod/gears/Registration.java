package com.redis.lettucemod.gears;

import lombok.Data;

import java.util.Map;

@Data
public class Registration {

    private String id;
    private String reader;
    private String description;
    private Data data;
    private String privateData;

    @lombok.Data
    public static class Data {
        private String mode;
        private long numTriggered;
        private long numSuccess;
        private long numFailures;
        private long numAborted;
        private String lastError;
        private Map<String, Object> args;
        private String status;
    }
}
