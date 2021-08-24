package com.redis.lettucemod.search;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Builder;
import lombok.Data;

@SuppressWarnings("rawtypes")
@Data
@Builder
public class SuggetOptions implements RediSearchArgument {

    private boolean fuzzy;
    private boolean withScores;
    private boolean withPayloads;
    private Long max;

    @Override
    public void build(RediSearchCommandArgs args) {
        if (fuzzy) {
            args.add(CommandKeyword.FUZZY);
        }
        if (withScores) {
            args.add(CommandKeyword.WITHSCORES);
        }
        if (withPayloads) {
            args.add(CommandKeyword.WITHPAYLOADS);
        }
        if (max != null) {
            args.add(CommandKeyword.MAX);
            args.add(max);
        }
    }

}
