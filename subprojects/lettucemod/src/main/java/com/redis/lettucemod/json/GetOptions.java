package com.redis.lettucemod.json;

import com.redis.lettucemod.json.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetOptions<K, V> implements RedisJSONArgument<K, V> {

    private V indent;
    private V newline;
    private V space;
    private boolean noEscape;

    @Override
    public void build(CommandArgs<K, V> args) {
        if (indent != null) {
            args.add(CommandKeyword.INDENT);
            args.addValue(indent);
        }
        if (newline != null) {
            args.add(CommandKeyword.NEWLINE);
            args.addValue(newline);
        }
        if (space != null) {
            args.add(CommandKeyword.SPACE);
            args.addValue(space);
        }
        if (noEscape) {
            args.add(CommandKeyword.NOESCAPE);
        }
    }

}
