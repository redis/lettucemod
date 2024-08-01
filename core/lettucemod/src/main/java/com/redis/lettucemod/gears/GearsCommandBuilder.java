package com.redis.lettucemod.gears;

import com.redis.lettucemod.RedisModulesCommandBuilder;
import com.redis.lettucemod.output.*;
import com.redis.lettucemod.protocol.GearsCommandKeyword;
import com.redis.lettucemod.protocol.GearsCommandType;
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
public class GearsCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    private static final String EXECUTION_ID = "Execution ID";

	public GearsCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(GearsCommandType type, CommandOutput<A, B, T> output) {
        return new Command<>(type, output);
    }

    protected <A, B, T> Command<A, B, T> createCommand(GearsCommandType type, CommandOutput<A, B, T> output, CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    private CommandArgs<K, V> args() {
        return new CommandArgs<>(codec);
    }

    public Command<K, V, String> abortExecution(String id) {
        notNull(id, EXECUTION_ID);
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(GearsCommandType.ABORTEXECUTION, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<V>> configGet(K... keys) {
        LettuceAssert.notEmpty(keys, "At least one key is required.");
        CommandArgs<K, V> args = args();
        for (K key : keys) {
            args.addKey(key);
        }
        return createCommand(GearsCommandType.CONFIGGET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, List<V>> configSet(Map<K, V> map) {
        notNull(map, "Map");
        LettuceAssert.isTrue(!map.isEmpty(), "At least one key/value is required.");
        CommandArgs<K, V> args = args();
        args.add(map);
        return createCommand(GearsCommandType.CONFIGSET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, String> dropExecution(String id) {
        notNull(id, EXECUTION_ID);
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(GearsCommandType.DROPEXECUTION, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Execution>> dumpExecutions() {
        return createCommand(GearsCommandType.DUMPEXECUTIONS, new ExecutionListOutput<>(codec));
    }

    public Command<K, V, List<Registration>> dumpRegistrations() {
        return createCommand(GearsCommandType.DUMPREGISTRATIONS, new RegistrationListOutput<>(codec));
    }

    public Command<K, V, ExecutionDetails> getExecution(String id) {
        return getExecution(id, null);
    }

    public Command<K, V, ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        notNull(id, EXECUTION_ID);
        CommandArgs<K, V> args = args();
        args.add(id);
        if (mode != null) {
            args.add(mode == ExecutionMode.SHARD ? GearsCommandKeyword.SHARD : GearsCommandKeyword.CLUSTER);
        }
        return createCommand(GearsCommandType.GETEXECUTION, new ExecutionDetailsOutput<>(codec), args);
    }

    public Command<K, V, ExecutionResults> getResults(String id) {
        return getResults(id, false);
    }

    public Command<K, V, ExecutionResults> getResultsBlocking(String id) {
        return getResults(id, true);
    }

    private Command<K, V, ExecutionResults> getResults(String id, boolean blocking) {
        notNull(id, EXECUTION_ID);
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(blocking ? GearsCommandType.GETRESULTSBLOCKING : GearsCommandType.GETRESULTS, new ExecutionResultsOutput<>(codec), args);
    }

    public Command<K, V, ExecutionResults> pyExecute(String function, V... requirements) {
        CommandArgs<K, V> args = pyExecuteArgs(function, false, requirements);
        return createCommand(GearsCommandType.PYEXECUTE, new ExecutionResultsOutput<>(codec), args);
    }

    public Command<K, V, String> pyExecuteUnblocking(String function, V... requirements) {
        CommandArgs<K, V> args = pyExecuteArgs(function, true, requirements);
        return createCommand(GearsCommandType.PYEXECUTE, new StatusOutput<>(codec), args);
    }

    private CommandArgs<K, V> pyExecuteArgs(String function, boolean unblocking, V... requirements) {
        notNull(function, "Function");
        notNull(requirements, "Requirements");
        CommandArgs<K, V> args = args();
        args.add(function);
        if (unblocking) {
            args.add(GearsCommandKeyword.UNBLOCKING);
        }
        if (requirements.length > 0) {
            args.add(GearsCommandKeyword.REQUIREMENTS);
            args.addValues(requirements);
        }
        return args;
    }

    public Command<K, V, List<Object>> trigger(String trigger, V... args) {
        notNull(trigger, "Trigger name");
        CommandArgs<K, V> commandArgs = args();
        commandArgs.add(trigger);
        commandArgs.addValues(args);
        return createCommand(GearsCommandType.TRIGGER, new ArrayOutput<>(codec), commandArgs);
    }

    public Command<K, V, String> unregister(String id) {
        notNull(id, "Registration ID");
        CommandArgs<K, V> args = args();
        args.add(id);
        return createCommand(GearsCommandType.UNREGISTER, new StatusOutput<>(codec), args);
    }


}
