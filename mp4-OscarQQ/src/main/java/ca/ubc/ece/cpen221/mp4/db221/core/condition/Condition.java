package ca.ubc.ece.cpen221.mp4.db221.core.condition;

import java.util.Map;

public abstract class Condition {

    protected String operator;
    protected String op1;
    protected String op2;

    protected Condition(String operator, String op1, String op2) {
        this.operator = operator;
        this.op1 = op1;
        this.op2 = op2;
    }

    public abstract boolean test(Map<String, String> row1, Map<String, String> row2);

    @Override
    public String toString() {
        return op1 + " " + operator + " " + stringValueOfOp2();
    }

    protected abstract String stringValueOfOp2();

}
