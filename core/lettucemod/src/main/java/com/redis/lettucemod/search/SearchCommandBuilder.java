package com.redis.lettucemod.search;

import com.redis.lettucemod.RedisModulesCommandBuilder;
import com.redis.lettucemod.protocol.SearchCommandType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.KeyListOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * Builder dedicated to RediSearch commands.
 */
public class SearchCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    public SearchCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(SearchCommandType type, CommandOutput<A, B, T> output,
            CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    private static void notNullIndex(Object index) {
        notNull(index, "Index");
    }

    public Command<K, V, List<Object>> info(K index) {
        notNullIndex(index);
        CommandArgs<K, V> args = args(index);
        return createCommand(SearchCommandType.INFO, new NestedMultiOutput<>(codec), args);
    }

    public Command<K, V, List<K>> list() {
        return new Command<>(SearchCommandType.LIST, new KeyListOutput<>(codec));
    }

}
