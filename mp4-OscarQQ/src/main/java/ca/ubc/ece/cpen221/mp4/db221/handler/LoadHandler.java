package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.regex.Matcher;

public class LoadHandler implements Handler {

    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

    /**
     * Load data from the file name.json and create a table with that name
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents
     *         the output of the command
     */
    @Override
    public String handle(DB221 database, String command) {
        Matcher matcher = Constants.LOAD_PATTERN.matcher(command);
        if (matcher.matches()) {
            String tableName = matcher.group(1);
            try (Reader reader = new InputStreamReader(new FileInputStream(tableName + ".json"))) {
                Table table = GSON.fromJson(reader, Table.class);
                database.newTable(table);
                return new QueryResult(table.getRows()).toString();
            } catch (IOException ex) {
                return new ErrorMessage("File \"" + tableName + ".json\" not Found").toString();
            }
        } else {
            return new ErrorMessage("Please check your statement").toString();
        }
    }

}
