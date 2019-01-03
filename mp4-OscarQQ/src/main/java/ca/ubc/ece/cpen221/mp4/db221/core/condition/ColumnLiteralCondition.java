package ca.ubc.ece.cpen221.mp4.db221.core.condition;

import java.util.Map;

public class ColumnLiteralCondition extends Condition {

    public ColumnLiteralCondition(String operator, String op1, String op2) {
        super(operator, op1, op2);
    }

    @Override
    public boolean test(Map<String, String> row, Map<String, String> dummy) {
        String val = row.get(op1);
        if ("<".equals(operator)) {
            return val.compareTo(op2) < 0;
        } else if (">".equals(operator)) {
            return val.compareTo(op2) > 0;
        } else if ("=".equals(operator)) {
            return val.compareTo(op2) == 0;
        } else if ("!=".equals(operator)) {
            return val.compareTo(op2) != 0;
        } else if ("<=".equals(operator)) {
            return val.compareTo(op2) <= 0;
        } else if (">=".equals(operator)) {
            return val.compareTo(op2) >= 0;
        } else {
            return false;
        }
    }

    @Override
    protected String stringValueOfOp2() {
        return "\"" + op2 + "\"";
    }

}
