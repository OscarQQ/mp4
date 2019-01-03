package ca.ubc.ece.cpen221.mp4.db221.core.condition;

import java.util.Map;

public class BetweenColumnsCondition extends Condition {

    public BetweenColumnsCondition(String operator, String op1, String op2) {
        super(operator, op1, op2);
    }

    @Override
    public boolean test(Map<String, String> row1, Map<String, String> row2) {
        String val1 = row1.get(op1);
        String val2 = row2.get(op2);
        if ("<".equals(operator)) {
            return val1.compareTo(val2) < 0;
        } else if (">".equals(operator)) {
            return val1.compareTo(val2) > 0;
        } else if ("=".equals(operator)) {
            return val1.compareTo(val2) == 0;
        } else if ("!=".equals(operator)) {
            return val1.compareTo(val2) != 0;
        } else if ("<=".equals(operator)) {
            return val1.compareTo(val2) <= 0;
        } else if (">=".equals(operator)) {
            return val1.compareTo(val2) >= 0;
        } else {
            return false;
        }
    }

    @Override
    protected String stringValueOfOp2() {
        return op2;
    }

}
