package com.redis.lettucemod.api.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sample {

    public final static long AUTO_TIMESTAMP = -1;

    private long timestamp;
    private double value;

    public boolean isAutoTimestamp() {
        return timestamp == AUTO_TIMESTAMP;
    }

    public static Sample auto(double value) {
        return new Sample(AUTO_TIMESTAMP, value);
    }

    public static Sample of(long timestamp, double value) {
        return new Sample(timestamp, value);
    }

}