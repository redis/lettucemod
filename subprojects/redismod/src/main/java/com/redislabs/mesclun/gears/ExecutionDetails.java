package com.redislabs.mesclun.gears;

import lombok.Data;

import java.util.List;

@Data
public class ExecutionDetails {

    private String shardId;
    private ExecutionPlan plan;

    @Data
    public static class ExecutionPlan {

        private String status;
        private long shardsReceived;
        private long shardsCompleted;
        private long results;
        private long errors;
        private long totalDuration;
        private long readDuration;
        private List<Step> steps;

        @Data
        public static class Step {

            private String type;
            private long duration;
            private String name;
            private String arg;

        }
    }

}
