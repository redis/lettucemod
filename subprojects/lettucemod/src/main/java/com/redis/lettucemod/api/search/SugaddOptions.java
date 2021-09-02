package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SugaddOptions<K, V> implements RediSearchArgument<K, V> {

    private boolean increment;
    private V payload;

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        if (increment) {
            args.add(SearchCommandKeyword.INCR);
        }
        if (payload != null) {
            args.add(SearchCommandKeyword.PAYLOAD);
            args.addValue(payload);
        }
    }
}
