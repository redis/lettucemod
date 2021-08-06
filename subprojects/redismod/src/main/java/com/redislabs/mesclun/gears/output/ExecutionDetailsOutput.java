package com.redislabs.mesclun.gears.output;

import com.redislabs.mesclun.gears.ExecutionDetails;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ExecutionDetailsOutput<K, V> extends CommandOutput<K, V, ExecutionDetails> {

    private String field;
    private ExecutionDetails.ExecutionPlan.Step step;

    public ExecutionDetailsOutput(RedisCodec<K, V> codec) {
        super(codec, new ExecutionDetails());
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (field == null) {
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("shard_id")) {
            output.setShardId(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("execution_plan")) {
            output.setPlan(new ExecutionDetails.ExecutionPlan());
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("status")) {
            output.getPlan().setStatus(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("steps")) {
            output.getPlan().setSteps(new ArrayList<>());
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("type")) {
            step = new ExecutionDetails.ExecutionPlan.Step();
            step.setType(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("name")) {
            step.setName(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("arg")) {
            step.setArg(decodeAscii(bytes));
            output.getPlan().getSteps().add(step);
            step = null;
            field = null;
        }
    }

    @Override
    public void set(long integer) {
        if (field.equals("shards_received")) {
            output.getPlan().setShardsReceived(integer);
            field = null;
            return;
        }
        if (field.equals("shards_completed")) {
            output.getPlan().setShardsCompleted(integer);
            field = null;
            return;
        }
        if (field.equals("results")) {
            output.getPlan().setResults(integer);
            field = null;
            return;
        }
        if (field.equals("errors")) {
            output.getPlan().setErrors(integer);
            field = null;
            return;
        }
        if (field.equals("total_duration")) {
            output.getPlan().setTotalDuration(integer);
            field = null;
            return;
        }
        if (field.equals("read_duration")) {
            output.getPlan().setReadDuration(integer);
            field = null;
            return;
        }
        if (field.equals("duration")) {
            step.setDuration(integer);
            field = null;
        }
    }

}
