package com.redis.lettucemod.api.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Suggestion<V> {

    private V string;
    private Double score;
    private V payload;

}
