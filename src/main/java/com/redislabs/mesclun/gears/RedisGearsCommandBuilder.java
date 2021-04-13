package com.redislabs.mesclun.gears;

import com.redislabs.mesclun.gears.protocol.CommandType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * Dedicated builder to build RedisGears commands.
 */
public class RedisGearsCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    static final String MUST_NOT_BE_NULL = "must not be null";

    public RedisGearsCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    private void assertNotNull(Object arg, String name) {
        LettuceAssert.notNull(arg, name + " " + MUST_NOT_BE_NULL);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CommandType type, CommandOutput<A, B, T> output) {
        return new Command<>(type, output);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CommandType type, CommandOutput<A, B, T> output, CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    private CommandArgs<K, V> args() {
        return new CommandArgs<>(codec);
    }

    public Command<K, V, String> pyExecute(String function, PyExecuteOptions options) {
        LettuceAssert.notNull(function, "A function is required.");
        CommandArgs<K, V> args = args();
        args.add(function);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.PYEXECUTE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Registration>> dumpRegistrations() {
        return createCommand(CommandType.DUMPREGISTRATIONS, new RegistrationListOutput<>(codec));
    }

}
