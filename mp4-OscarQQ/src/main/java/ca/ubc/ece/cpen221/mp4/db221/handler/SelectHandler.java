package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.BetweenColumnsCondition;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.ColumnLiteralCondition;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.Condition;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectHandler implements Handler {
    /**
     *  a table that selects information from other tables
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents
     *          the output of the command
     */
    @Override
    public String handle(DB221 database, String command) {
        Matcher matcher = Constants.SELECT_PATTERN.matcher(command);
        if (matcher.matches()) {
            String[] columns = matcher.group(1).split(",");
            String[] tables = matcher.group(3).split(",");
            for (int i = 0; i < columns.length; i++) {
                columns[i] = columns[i].trim();
            }
            for (int i = 0; i < tables.length; i++) {
                tables[i] = tables[i].trim();
            }
            List<String> boolOperators = new ArrayList<>();
            List<Condition> conditions = new ArrayList<>();
            String[] conditionClauses = new String[0];
            if (matcher.group(6) != null) {
                conditionClauses = matcher.group(6).split("\\s+(and|or)\\s+");
                for (int i = 0; i < conditionClauses.length; i++) {
                    conditionClauses[i] = conditionClauses[i].trim();
                }
                Matcher boolOperatorMatcher = Pattern.compile("\\s+(and|or)\\s+").matcher(command);
                while (boolOperatorMatcher.find()) {
                    boolOperators.add(boolOperatorMatcher.group().trim());
                }
                for (String conditionClause : conditionClauses) {
                    Matcher conditionMatcher = Pattern.compile(Constants.CONDITION_REGEX).matcher(conditionClause.trim());
                    conditionMatcher.matches();
                        String op1 = conditionMatcher.group(1);
                        String operator = conditionMatcher.group(2);
                        String op2 = conditionMatcher.group(9);
                        if (op2.startsWith("\"")) {
                            conditions.add(new ColumnLiteralCondition(operator, op1, op2.substring(1, op2.length() - 1)));
                        } else {
                            conditions.add(new BetweenColumnsCondition(operator, op1, op2));
                        }
                }
            }
            if (tables.length == 1) {
                Table table = database.getTable(tables[0]);
                if (table != null) {
                    if (conditions.size() > 0) {
                        Set<LinkedHashMap<String, String>> selectedRows = new HashSet<>();
                        for (LinkedHashMap<String, String> row : table.getRows()) {
                            boolean satisfied = conditions.get(0).test(row, new HashMap<>());
                            for (int i = 1; i < conditions.size(); i++) {
                                boolean testRes = conditions.get(i).test(row, new HashMap<>());
                                String boolOperator = boolOperators.get(i - 1);
                                if (boolOperator.equals("and")) {
                                    satisfied = satisfied && testRes;
                                } else {
                                    satisfied = satisfied || testRes;
                                }
                            }
                            if (satisfied) {
                                LinkedHashMap<String, String> selectedRow = new LinkedHashMap<>();
                                for (String column : columns) {
                                    if (table.containsColumn(column)) {
                                        selectedRow.put(column, row.get(column));
                                    } else {
                                        return new ErrorMessage("Table \"" + tables[0] + "\" does not have column \"" + column + "\"").toString();
                                    }
                                }
                                selectedRows.add(selectedRow);
                            }
                        }
                        return new QueryResult(selectedRows).toString();
                    } else {
                        // no condition, select all
                        Set<LinkedHashMap<String, String>> selectedRows = new HashSet<>();
                        for (Map<String, String> row : table.getRows()) {
                            LinkedHashMap<String, String> selectedRow = new LinkedHashMap<>();
                            for (String column : columns) {
                                if (table.containsColumn(column)) {
                                    selectedRow.put(column, row.get(column));
                                } else {
                                    return new ErrorMessage("Table \"" + tables[0] + "\" does not have column \"" + column + "\"").toString();
                                }
                            }
                            selectedRows.add(selectedRow);
                        }
                        return new QueryResult(selectedRows).toString();
                    }
                } else {
                    return new ErrorMessage("Table \"" + tables[0] + "\" does not exist").toString();
                }
            } else {
                Table tableA = database.getTable(tables[0]);
                if (tableA != null) {
                    Table tableB = database.getTable(tables[1]);
                    if (tableB != null) {
                        Set<LinkedHashMap<String, String>> rows = new HashSet<>();
                        Set<String> columnsOfA = tableA.columns();
                        Set<String> columnsOfB = tableB.columns();
                        columnsOfA.retainAll(columnsOfB);
                        if (!columnsOfA.isEmpty()) {
                            // common columns
                            for (Map<String, String> rowInA : tableA.getRows()) {
                                for (Map<String, String> rowInB : tableB.getRows()) {
                                    boolean canJoin = true;
                                    for (String commonColumn : columnsOfA) {
                                        if (!rowInA.get(commonColumn).equals(rowInB.get(commonColumn))) {
                                            canJoin = false;
                                            break;
                                        }
                                    }
                                    if (canJoin) {
                                        LinkedHashMap<String, String> combinedRow = new LinkedHashMap<>();
                                        for (Map.Entry<String, String> entryA : rowInA.entrySet()) {
                                            combinedRow.put(entryA.getKey(), entryA.getValue());
                                        }
                                        for (Map.Entry<String, String> entryB : rowInB.entrySet()) {
                                            if (!columnsOfA.contains(entryB.getKey())) {
                                                combinedRow.put(entryB.getKey(), entryB.getValue());
                                            }
                                        }
                                        rows.add(combinedRow);
                                    }
                                }
                            }
                        } else {
                            // no common columns
                            for (Map<String, String> rowInA : tableA.getRows()) {
                                for (Map<String, String> rowInB : tableB.getRows()) {
                                    LinkedHashMap<String, String> combinedRow = new LinkedHashMap<>();
                                    for (Map.Entry<String, String> entryA : rowInA.entrySet()) {
                                        combinedRow.put(entryA.getKey(), entryA.getValue());
                                    }
                                    for (Map.Entry<String, String> entryB : rowInB.entrySet()) {
                                        combinedRow.put(entryB.getKey(), entryB.getValue());
                                    }
                                    rows.add(combinedRow);
                                }
                            }
                        }
                        if (conditionClauses.length > 0) {
                            Set<LinkedHashMap<String, String>> selectedRows = new HashSet<>();
                            for (LinkedHashMap<String, String> row : rows) {
                                boolean satisfied = conditions.get(0).test(row, row);
                                for (int i = 1; i < conditions.size(); i++) {
                                    boolean testRes = conditions.get(i).test(row, row);
                                    String boolOperator = boolOperators.get(i - 1);
                                    if (boolOperator.equals("and")) {
                                        satisfied = satisfied && testRes;
                                    } else {
                                        satisfied = satisfied || testRes;
                                    }
                                }
                                if (satisfied) {
                                    LinkedHashMap<String, String> selectedRow = new LinkedHashMap<>();
                                    for (String column : columns) {
                                        selectedRow.put(column, row.get(column));
                                    }
                                    selectedRows.add(selectedRow);
                                }
                            }
                            return new QueryResult(selectedRows).toString();
                        } else {
                            // no condition, select all
                            Set<LinkedHashMap<String, String>> selectedRows = new HashSet<>();
                            for (Map<String, String> row : rows) {
                                LinkedHashMap<String, String> selectedRow = new LinkedHashMap<>();
                                for (String column : columns) {
                                    String val = row.getOrDefault(column, null);
                                    if (val != null) {
                                        selectedRow.put(column, row.get(column));
                                    } else {
                                        return new ErrorMessage("Table " + tables[0] + " JOIN " + tables[1] + " does not have column \"" + column + "\"").toString();
                                    }
                                }
                                selectedRows.add(selectedRow);
                            }
                            return new QueryResult(selectedRows).toString();
                        }
                    } else {
                        return new ErrorMessage("Table " + tables[1] + " does not exist").toString();
                    }
                } else {
                    return new ErrorMessage("Table " + tables[0] + " does not exist").toString();
                }
            }
        } else {
            return new ErrorMessage("Please check your statement").toString();
        }
    }

}
