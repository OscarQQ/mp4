package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;
import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.regex.Matcher;

public class StoreHandler implements Handler {

    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();

    /**
     * Store the data from the table name into the file name.json.
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a string in JSON format that represents the output of the command
     * @throws IOException
     */
    @Override
    public String handle(DB221 database, String command) throws IOException {
        Matcher matcher = Constants.STORE_PATTERN.matcher(command);
        if (matcher.matches()) {
            String tableName = matcher.group(1);
            if (database.tableExists(tableName)) {
                Table table = database.getTable(tableName);
                File output = new File(tableName + ".json");
                output.createNewFile();
                Writer writer = new FileWriter(output);
                writer.write(GSON.toJson(table));
                writer.flush();
                writer.close();
                return new QueryResult(table.getRows()).toString();
            } else {
                return new ErrorMessage("Table \"" + tableName + "\" does not exist").toString();
            }
        } else {
            return new ErrorMessage("Please check your statement").toString();
        }
    }

    public Gson getJsonHelper () {
        return GSON;
    }

}
