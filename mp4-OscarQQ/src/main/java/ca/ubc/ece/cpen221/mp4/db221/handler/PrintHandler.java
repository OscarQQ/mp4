package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;

import java.util.regex.Matcher;

public class PrintHandler implements Handler {
    /**
     * Print all rows of the table with the given name
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents
     *          the output of the command
     */
    @Override
    public String handle(DB221 database, String command) {
        Matcher matcher = Constants.PRINT_PATTERN.matcher(command);
        if (matcher.matches()) {
            String tableName = matcher.group(1);
            if (database.tableExists(tableName)) {
                return database.getTable(tableName).print();
            } else {
                return new ErrorMessage("Table \"" + tableName + "\" does not exist").toString();
            }
        } else {
            return new ErrorMessage("Please check your statement").toString();
        }
    }

}
