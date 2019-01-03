package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

public class CreateHandler implements Handler {
    /**
     * Create an empty table with the given name
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents
     *         the output of the command
     */
    @Override
    public String handle(DB221 database, String command) {
        Matcher matcher = Constants.CREATE_PATTERN.matcher(command);
        if (matcher.matches()) {
            String tableName = matcher.group(1);
            String tableDef = matcher.group(2);
            tableDef = tableDef.substring(1, tableDef.length() - 1);
            String[] tmp = tableDef.split(",");

            // check if there is any duplicate column names
            Set<String> aux = new HashSet<>(Arrays.asList(tmp));
            if (aux.size() != tmp.length) {
                return new ErrorMessage("There must not be any duplicate column names").toString();
            }

            String[] columnNames = new String[tmp.length];
            for (int i = 0; i < tmp.length; i++) {
                columnNames[i] = tmp[i].trim();
            }
            Table table = new Table(tableName, Arrays.asList(columnNames));
            database.newTable(table);
            return new QueryResult(table.getRows()).toString();
        } else {
            return new ErrorMessage("Syntax error, please check your statement").toString();
        }
    }

}
