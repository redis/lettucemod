package com.redis.search.query.filter;

import java.util.StringJoiner;

/**
 * Represents a logical OR condition between two {@link Condition} objects.
 * <p>
 * This class generates a query that joins the left and right conditions using
 * the OR operator ('|') in Redis Search query syntax.
 * <p>
 * Example output:
 * <pre>{@code
 * (@field1:{value1})|(@field2:{value2})
 * }</pre>
 */
public class Or extends CompositeCondition {

    /**
     * The delimiter used for joining two conditions with OR.
     */
    public static final String DELIMITER = "|";

    /**
     * Constructs an {@code Or} condition from two {@link Condition} objects.
     *
     * @param left  The left condition
     * @param right The right condition
     */
    public Or(Condition left, Condition right) {
        super(DELIMITER, left, right);
    }

    /**
     * Builds a logical OR query between two sub-conditions.
     * <p>
     * Logic:
     * 1. If both left and right queries are present, returns them in the format "(@leftQuery)|(@rightQuery)".
     * 2. If only one of them is present, returns that query directly wrapped in parentheses.
     * 3. If both are missing or empty, returns an empty string.
     *
     * @return A string representing the logical query.
     */
    @Override
    public String getQuery() {
        String leftQuery = (left != null) ? left.getQuery() : null;
        String rightQuery = (right != null) ? right.getQuery() : null;

        // Treat empty or whitespace strings as null
        if (rightQuery != null && rightQuery.trim().isEmpty()) {
            rightQuery = null;
        }

        boolean hasLeft = leftQuery != null && !leftQuery.isEmpty();
        boolean hasRight = rightQuery != null && !rightQuery.isEmpty();

        if (hasLeft && hasRight) {
            // Both sides present: (@leftQuery)|(@rightQuery)
            return String.format("(%s)|(%s)", leftQuery, rightQuery);
        } else if (hasLeft) {
            // Only left side present: (@leftQuery)
            return String.format("(%s)", leftQuery);
        } else if (hasRight) {
            // Only right side present: (@rightQuery)
            return String.format("(%s)", rightQuery);
        } else {
            // Both sides missing or blank
            return "";
        }
    }
}
