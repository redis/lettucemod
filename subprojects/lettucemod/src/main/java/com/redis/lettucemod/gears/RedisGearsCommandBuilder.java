package com.redis.lettucemod.gears;

import com.redis.lettucemod.gears.output.*;
import com.redis.lettucemod.gears.protocol.CommandKeyword;
import com.redis.lettucemod.gears.protocol.CommandType;
import com.redis.lettucemod.RedisModulesCommandBuilder;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;
import java.util.Map;

/**
 * Builder dedicated to RedisGears commands.
 */
@SuppressWarnings("unchecked")
public class RedisGearsCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

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
        notNull(id, "execution ID");
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
        notNull(map, "Map");
        LettuceAssert.isTrue(!map.isEmpty(), "At least one key/value is required.");
        CommandArgs<K, V> args = args();
        args.add(map);
        return createCommand(CommandType.CONFIGSET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, String> dropExecution(String id) {
        notNull(id, "Execution ID");
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
        notNull(id, "Execution ID");
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
        notNull(id, "Execution ID");
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
        notNull(function, "Function");
        notNull(requirements, "Requirements");
        CommandArgs<K, V> args = args();
        args.add(function);
        if (unblocking) {
            args.add(CommandKeyword.UNBLOCKING);
        }
        if (requirements.length > 0) {
            args.add(CommandKeyword.REQUIREMENTS);
            args.addValues(requirements);
        }
        return args;
    }

    public Command<K, V, List<Object>> trigger(String trigger, V... args) {
        notNull(trigger, "Trigger name");
        CommandArgs<K, V> commandArgs = args();
        commandArgs.add(trigger);
        commandArgs.addValues(args);
        return createCommand(CommandType.TRIGGER, new ArrayOutput<>(codec), commandArgs);
    }

    public Command<K, V, String> unregister(String id) {
        notNull(id, "Registration ID");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(CommandType.UNREGISTER, new StatusOutput<>(codec), args);
    }


}
