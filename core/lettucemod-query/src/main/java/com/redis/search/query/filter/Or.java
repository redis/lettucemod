package com.redis.search.query.filter;

public class Or extends CompositeCondition {

    public static final String DELIMITER = "|";
    private static final String FORMAT = "((%s)|(%s))";

    public Or(Condition left, Condition right) {
        super(DELIMITER, left, right);
    }

    public String getQuery() {
        String leftQuery = left != null && left.getQuery() != null ? left.getQuery() : "";
        String rightQuery = right != null && right.getQuery() != null ? right.getQuery() : "";
        return String.format(FORMAT,leftQuery, rightQuery);
    }

}
