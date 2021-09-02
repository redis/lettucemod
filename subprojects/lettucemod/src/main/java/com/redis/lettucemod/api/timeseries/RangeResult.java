package com.redis.lettucemod.api.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class RangeResult<K, V> {

    private K key;
    private Map<K, V> labels;
    private List<Sample> samples;

}
