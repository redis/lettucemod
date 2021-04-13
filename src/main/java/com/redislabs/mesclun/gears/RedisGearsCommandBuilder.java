package com.redislabs.mesclun.gears;

import com.redislabs.mesclun.gears.output.*;
import com.redislabs.mesclun.gears.protocol.CommandKeyword;
import com.redislabs.mesclun.gears.protocol.CommandType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;
import java.util.Map;

import static com.redislabs.mesclun.gears.protocol.CommandKeyword.REQUIREMENTS;
import static com.redislabs.mesclun.gears.protocol.CommandKeyword.UNBLOCKING;

/**
 * Dedicated builder to build RedisGears commands.
 */
@SuppressWarnings("unchecked")
public class RedisGearsCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    public RedisGearsCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
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

    public Command<K, V, String> abortExecution(String id) {
        LettuceAssert.notNull(id, "An execution ID is required.");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(CommandType.ABORTEXECUTION, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<V>> configGet(K... keys) {
        LettuceAssert.notEmpty(keys, "At least one key is required.");
        CommandArgs<K, V> args = args();
        for (K key : keys) {
            args.addKey(key);
        }
        return createCommand(CommandType.CONFIGGET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, List<V>> configSet(Map<K, V> map) {
        LettuceAssert.notNull(map, "A key/value map is required.");
        LettuceAssert.isTrue(!map.isEmpty(), "At least one key/value is required.");
        CommandArgs<K, V> args = args();
        args.add(map);
        return createCommand(CommandType.CONFIGSET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, String> dropExecution(String id) {
        LettuceAssert.notNull(id, "An execution ID is required.");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(CommandType.DROPEXECUTION, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Execution>> dumpExecutions() {
        return createCommand(CommandType.DUMPEXECUTIONS, new ExecutionListOutput<>(codec));
    }

    public Command<K, V, List<Registration>> dumpRegistrations() {
        return createCommand(CommandType.DUMPREGISTRATIONS, new RegistrationListOutput<>(codec));
    }

    public Command<K, V, ExecutionDetails> getExecution(String id) {
        return getExecution(id, null);
    }

    public Command<K, V, ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        LettuceAssert.notNull(id, "An execution ID is required.");
        CommandArgs<K, V> args = args();
        args.add(id);
        if (mode != null) {
            args.add(mode == ExecutionMode.SHARD ? CommandKeyword.SHARD : CommandKeyword.CLUSTER);
        }
        return createCommand(CommandType.GETEXECUTION, new ExecutionDetailsOutput<>(codec), args);
    }

    public Command<K, V, ExecutionResults> getResults(String id) {
        return getResults(id, false);
    }

    public Command<K, V, ExecutionResults> getResultsBlocking(String id) {
        return getResults(id, true);
    }

    private Command<K, V, ExecutionResults> getResults(String id, boolean blocking) {
        LettuceAssert.notNull(id, "An execution ID is required.");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(blocking ? CommandType.GETRESULTSBLOCKING : CommandType.GETRESULTS, new ExecutionResultsOutput<>(codec), args);
    }

    public Command<K, V, ExecutionResults> pyExecute(String function, V... requirements) {
        CommandArgs<K, V> args = pyExecuteArgs(function, false, requirements);
        return createCommand(CommandType.PYEXECUTE, new ExecutionResultsOutput<>(codec), args);
    }

    public Command<K, V, String> pyExecuteUnblocking(String function, V... requirements) {
        CommandArgs<K, V> args = pyExecuteArgs(function, true, requirements);
        return createCommand(CommandType.PYEXECUTE, new StatusOutput<>(codec), args);
    }

    private CommandArgs<K, V> pyExecuteArgs(String function, boolean unblocking, V... requirements) {
        LettuceAssert.notNull(function, "A function is required.");
        LettuceAssert.notNull(requirements, "Requirements must not be null.");
        CommandArgs<K, V> args = args();
        args.add(function);
        if (unblocking) {
            args.add(UNBLOCKING);
        }
        if (requirements.length > 0) {
            args.add(REQUIREMENTS);
            args.addValues(requirements);
        }
        return args;
    }

    public Command<K, V, List<Object>> trigger(String trigger, V... args) {
        LettuceAssert.notNull(trigger, "A trigger name is required.");
        CommandArgs<K, V> commandArgs = args();
        commandArgs.add(trigger);
        commandArgs.addValues(args);
        return createCommand(CommandType.TRIGGER, new ArrayOutput<>(codec), commandArgs);
    }

    public Command<K, V, String> unregister(String id) {
        LettuceAssert.notNull(id, "A registration ID is required.");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(CommandType.UNREGISTER, new StatusOutput<>(codec), args);
    }


}
