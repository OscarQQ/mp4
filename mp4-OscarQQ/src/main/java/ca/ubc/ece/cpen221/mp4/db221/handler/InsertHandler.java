package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertHandler implements Handler {
    /**
     * Add new rows to the given table
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents
     * the output of the command
     */
    @Override
    public String handle(DB221 database, String command) {
        Matcher matcher = Constants.INSERT_PATTERN.matcher(command);
        if (matcher.matches()) {
            String tableName = matcher.group(1);
            if (database.tableExists(tableName)) {
                Table table = database.getTable(tableName);
                Matcher tuplesMatcher = Pattern.compile(Constants.VALUE_REGEX).matcher(matcher.group(2));
                while (tuplesMatcher.find()) {
                    String[] tuples = tuplesMatcher.group().split("\\),\\s*\\(");
                    tuples[0] = tuples[0].substring(1);
                    tuples[tuples.length - 1] = tuples[tuples.length - 1].substring(0, tuples[tuples.length - 1].length() - 1);
                    for (String tuple1 : tuples) {
                        String[] tuple = tuple1.trim().split(",");
                        for (int j = 0; j < tuple.length; j++) {
                            tuple[j] = tuple[j].trim();
                        }
                        if (!table.insert(tuple)) {
                            return new ErrorMessage("Columns do not match").toString();
                        }
                    }
                }
                return new QueryResult(table.getRows()).toString();
            } else {
                return new ErrorMessage("Table \"" + tableName + "\" does not exist").toString();
            }
        } else {
            return new ErrorMessage("Please check your statement").toString();
        }
    }

}
