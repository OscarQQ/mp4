package ca.ubc.ece.cpen221.mp4.db221;

import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.handler.*;
import ca.ubc.ece.cpen221.mp4.db221.util.Constants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class DB221 {

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String OUTPUT_DIR = System.getProperty("user.dir") + FILE_SEPARATOR;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMMdd-HHmm", Locale.US);
    private static final String PROMPT = "> ";

    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();

    private String databaseName;
    private Map<String, Table> tables;

    /**
     * the constructor of the database
     * @param dbName a string that represents the name of the database
     * @throws IOException
     */
    public DB221(String dbName) throws IOException {
        this(dbName, new ArrayList<>());
    }

    /**
     * the second constructor of the database, the second way of creating it
     * @param dbName the name of the database
     * @param tableList the list of tables that the database is created from
     * @throws IOException
     */
    public DB221(String dbName, List<String> tableList) throws IOException {
        this.databaseName = dbName;
        this.tables = new HashMap<>();
        for (String tableName : tableList) {
            int tableNameStartIndex = Math.max(tableName.lastIndexOf('/') + 1, 0);
            tables.put(tableName.substring(tableNameStartIndex, tableName.lastIndexOf('.')), readDataFile(tableName));
        }
    }

    /**
     * launch an interactive session with the database
     * @param args the name of the database
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Please input the name of the database");
        } else {
            List<String> tables = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
            DB221 myDb = new DB221(args[0], tables);

            Scanner in = new Scanner(System.in);
            System.out.print(PROMPT);
            String command;
            while (in.hasNextLine()) {
                command = in.nextLine().trim();
                if (!"".equals(command)) {
                    if (Constants.EXIT_PATTERN.matcher(command).matches()) {
                        break;
                    } else {
                        String jsonOutput = myDb.exec(command);
                        System.out.println(jsonOutput);
                    }
                }
                System.out.print(PROMPT);
            }
        }
    }

    /**
     * read a data file
     * @param tableDataFilePath the path of the data we want to read
     * @return a table we read from a datafile
     * @throws FileNotFoundException if the datafile doesn't exist
     */
    Table readDataFile(String tableDataFilePath) throws FileNotFoundException {
        Reader reader = new InputStreamReader(new FileInputStream(tableDataFilePath));
        return GSON.fromJson(reader, Table.class);
    }

    /**
     * Take a string and execute
     * @param command the command we want to execute
     * @return a string in JSON format that represents
     *      *                the output of the command
     * @throws UnsupportedOperationException if the operation is unsupported
     * @throws IOException if there are errors with the inputs or outputs
     */
    public String exec(String command) throws UnsupportedOperationException, IOException {
        String type = command.split("\\s+")[0];
        if (Constants.CREATE.equals(type)) {
            return new CreateHandler().handle(this, command);
        } else if (Constants.INSERT.equals(type)) {
            return new InsertHandler().handle(this, command);
        } else if (Constants.LOAD.equals(type)) {
            return new LoadHandler().handle(this, command);
        } else if (Constants.PRINT.equals(type)) {
            return new PrintHandler().handle(this, command);
        } else if (Constants.SELECT.equals(type)) {
            return new SelectHandler().handle(this, command);
        } else if (Constants.STORE.equals(type)) {
            return new StoreHandler().handle(this, command);
        } else {
            System.err.println("Unsupported Operation: \"" + type + "\", please check your statement. ");
            return null;
        }
    }

    /**
     * writes all tables to the appropriate JSON files
     * @throws IOException if there is any errors
     */
    public void snapshot() throws IOException {
        String snapshotDirPath = OUTPUT_DIR + databaseName
                + "-" + "snapshot" + "-"
                + DATE_FORMATTER.format(new Date())
                + FILE_SEPARATOR;
        File snapshotDir = new File(snapshotDirPath);
        snapshotDir.mkdirs();
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            File output = new File(snapshotDirPath + entry.getKey() + ".json");
            output.createNewFile();
            Writer writer = new FileWriter(output);
            writer.write(GSON.toJson(entry.getValue()));
            writer.flush();
            writer.close();
        }
    }

    /**
     * Create a new table in the database
     * @param table a table we want
     */
    public void newTable(Table table) {
        tables.put(table.getName(), table);
    }

    /**
     * Get one of the table in the database
     * @param tableName the table's name as a string
     * @return the table we want
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * check if a table exists in the database
     * @param tableName is a string represents the name of the table
     * @return true if the table is included, false otherwise
     */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

}
