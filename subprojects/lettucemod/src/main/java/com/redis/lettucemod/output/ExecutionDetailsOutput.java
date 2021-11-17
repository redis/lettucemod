package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.redis.lettucemod.gears.ExecutionDetails;

public class ExecutionDetailsOutput<K, V> extends CommandOutput<K, V, ExecutionDetails> {

    private String field;
    private ExecutionDetails.ExecutionPlan.Step step;

    public ExecutionDetailsOutput(RedisCodec<K, V> codec) {
        super(codec, new ExecutionDetails());
    }
    
    private boolean fieldEquals(String name) {
        return name.equals(field);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (field == null) {
            field = decodeAscii(bytes);
            return;
        }
        if (fieldEquals("shard_id")) {
            output.setShardId(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("execution_plan")) {
            output.setPlan(new ExecutionDetails.ExecutionPlan());
            field = decodeAscii(bytes);
            return;
        }
        if (fieldEquals("status")) {
            output.getPlan().setStatus(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("steps")) {
            output.getPlan().setSteps(new ArrayList<>());
            field = decodeAscii(bytes);
            return;
        }
        if (fieldEquals("type")) {
            step = new ExecutionDetails.ExecutionPlan.Step();
            step.setType(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("name")) {
            step.setName(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("arg")) {
            step.setArg(decodeAscii(bytes));
            output.getPlan().getSteps().add(step);
            step = null;
            field = null;
        }
    }

    @Override
    public void set(long integer) {
        if (fieldEquals("shards_received")) {
            output.getPlan().setShardsReceived(integer);
            field = null;
            return;
        }
        if (fieldEquals("shards_completed")) {
            output.getPlan().setShardsCompleted(integer);
            field = null;
            return;
        }
        if (fieldEquals("results")) {
            output.getPlan().setResults(integer);
            field = null;
            return;
        }
        if (fieldEquals("errors")) {
            output.getPlan().setErrors(integer);
            field = null;
            return;
        }
        if (fieldEquals("total_duration")) {
            output.getPlan().setTotalDuration(integer);
            field = null;
            return;
        }
        if (fieldEquals("read_duration")) {
            output.getPlan().setReadDuration(integer);
            field = null;
            return;
        }
        if (fieldEquals("duration")) {
            step.setDuration(integer);
            field = null;
        }
    }

}
