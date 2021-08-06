package com.redislabs.lettucemod.gears.output;

import java.util.List;

public class ExecutionResults {

    private boolean ok;
    private List<Object> results;
    private List<String> errors;

    public boolean isOk() {
        return !isError();
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public boolean isError() {
        return !ok && errors != null && !errors.isEmpty();
    }

    public List<Object> getResults() {
        return results;
    }

    public void setResults(List<Object> results) {
        this.results = results;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}
