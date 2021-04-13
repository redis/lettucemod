package com.redislabs.mesclun.gears.output;

import lombok.Data;

import java.util.List;

@Data
public class ExecutionResults {

    private List<Object> results;
    private List<String> errors;

    public boolean isError() {
        if (errors == null || errors.isEmpty()) {
            return false;
        }
        return !"OK".equals(errors.get(0));
    }

    public String getError() {
        if (isError()) {
            return errors.get(0);
        }
        return null;
    }

}
