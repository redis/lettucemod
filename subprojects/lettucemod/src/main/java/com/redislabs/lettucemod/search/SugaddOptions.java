package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SugaddOptions<K, V> implements RediSearchArgument<K, V> {

    private boolean increment;
    private V payload;

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        if (increment) {
            args.add(CommandKeyword.INCR);
        }
        if (payload != null) {
            args.add(CommandKeyword.PAYLOAD);
            args.addValue(payload);
        }
    }
}
