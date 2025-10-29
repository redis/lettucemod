package com.redis.search.query.filter;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Represents a logical OR condition composed of multiple {@link Condition} elements.
 * <p>
 * This class generates a query string where each condition is wrapped in parentheses
 * and separated by the OR operator ('|'), suitable for use in Redis Search queries.
 * <p>
 * Example output:
 * <pre>{@code
 *     ( (query1)|(query2)|(query3) )
 * }</pre>
 */
public class OrList implements Condition {

    /**
     * The delimiter used to join conditions in an OR clause.
     */
    public static final String OR_LIST_CONDITION_FORMAT = "|";

    private final List<Condition> conditions;

    /**
     * Constructs an {@code OrList} with the given conditions.
     *
     * @param conditions One or more {@link Condition} objects to be ORed together.
     */
    public OrList(Condition... conditions) {
        this.conditions = Arrays.asList(conditions);
    }

    /**
     * Builds and returns the query string representing a logical OR condition.
     * <p>
     * Logic:
     * <ul>
     *   <li>Each valid (non-null and non-empty) condition query is wrapped in parentheses.</li>
     *   <li>All valid conditions are joined using the OR operator {@code |}.</li>
     *   <li>If fewer than two valid conditions are present, the query is returned without wrapping the entire string in extra parentheses.</li>
     *   <li>If no valid conditions are present, an empty string is returned.</li>
     * </ul>
     *
     * @return A formatted query string representing the logical OR of valid conditions,
     * or an empty string if none exist.
     */
    @Override
    public String getQuery() {
        if (conditions == null || conditions.isEmpty()) {
            return "";
        }

        StringJoiner joiner = new StringJoiner(OR_LIST_CONDITION_FORMAT);
        int validConditionCounter = 0;
        for (Condition condition : conditions) {
            if (condition == null) {
                continue;
            }

            String query = condition.getQuery();
            if (query != null && !query.isEmpty()) {
                joiner.add("(" + query + ")");
                validConditionCounter++;
            }
        }
        if (joiner.length() == 0 || validConditionCounter < 2) {
            return joiner.toString();
        }

        return "(" + joiner.toString() + ")";
    }
}
