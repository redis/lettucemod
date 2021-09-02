package com.redis.lettucemod.api.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GetResult<K, V> {

    private K key;
    private Map<K, V> labels;
    private Sample sample;

}
