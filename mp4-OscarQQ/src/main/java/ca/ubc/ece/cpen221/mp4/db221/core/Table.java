package ca.ubc.ece.cpen221.mp4.db221.core;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.*;


public class Table {

    @Expose
    @SerializedName("table_name")
    private String tableName;

    @Expose
    @SerializedName("table")
    private Set<LinkedHashMap<String, String>> rows;

    private ArrayList<String> columnsDefinedOrder;

//    private LinkedHashMap<String, String> emptyRow = new LinkedHashMap<>();

    /**
     * the constructor of a Table
     * create the table rows as a Hashset
     */
    public Table() {
        this.tableName = "";
        this.rows = new HashSet<>();
        this.columnsDefinedOrder = new ArrayList<>();
    }

    /**
     * the second constructor of a table
     * create a table as well
     *
     * @param tableName is a string
     * @param columnNames is a list of column names
     */
    public Table(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        this.rows = new HashSet<>();
        this.columnsDefinedOrder = new ArrayList<>(columnNames);
    }

    /**
     * Check if a row can be inserted
     * @param tuple is a string that defines a row
     * @return  a boolean variable: true if it can be inserted, false otherwise
     */
    public boolean insert(String[] tuple) {
        if (columnsDefinedOrder.size() <= 0) {
            columnsDefinedOrder = rows.stream()
                    .findFirst()
                    .map(x -> new ArrayList<>(x.keySet()))
                    .orElse(columnsDefinedOrder);
        }
        if (tuple.length == columnsDefinedOrder.size()) {
            LinkedHashMap<String, String> r = new LinkedHashMap<>();
            for (int i = 0; i < columnsDefinedOrder.size(); i++) {
                String columnName = columnsDefinedOrder.get(i);
                r.put(columnName, tuple[i]);
            }
            return rows.add(r);
        } else {
            return false;
        }
    }

    /**
     * get the name of a table
     * @return a string of the table's name
     */
    public String getName() {
        return tableName;
    }

    /**
     * get the rows of a table as a set
     * @return a set of Linkedhashmap that have strings as its keys and values
     */
    public Set<LinkedHashMap<String, String>> getRows() {
        return rows;
    }

    /**
     * check if a Column exists in a table
     * @param column is a string of a column's name
     * @return a boolean variable: true if the column exists, false otherwise
     */
    public boolean containsColumn(String column) {
        if (columnsDefinedOrder.size() <= 0) {
            columnsDefinedOrder = rows.stream()
                    .findFirst()
                    .map(x -> new ArrayList<>(x.keySet()))
                    .orElse(columnsDefinedOrder);
        }
        return columnsDefinedOrder.contains(column);
    }

    /**
     *Find columns of a table
     * @return a set of strings that represent each columns
     */
    public Set<String> columns() {
        if (columnsDefinedOrder.size() <= 0) {
            columnsDefinedOrder = rows.stream()
                    .findFirst()
                    .map(x -> new ArrayList<>(x.keySet()))
                    .orElse(columnsDefinedOrder);
        }

        return new LinkedHashSet<>(columnsDefinedOrder);
    }

    /**
     * assign the object to a string
     * @return a string that represents the table that we can do some operations on it
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(System.lineSeparator());

        if (columnsDefinedOrder.size() <= 0) {
            columnsDefinedOrder = rows.stream()
                    .findFirst()
                    .map(x -> new ArrayList<>(x.keySet()))
                    .orElse(columnsDefinedOrder);
        }

        Map<String, Integer> widthEachColumn = new HashMap<>();
        for (String columnName : columnsDefinedOrder) {
            widthEachColumn.put(columnName, 0);
        }

        for (Map<String, String> row : rows) {
            for (String columnName : columnsDefinedOrder) {
                int length = row.get(columnName).length();
                if (length > widthEachColumn.get(columnName)) {
                    widthEachColumn.put(columnName, length);
                }
            }
        }

        int tableWidth = 0;
        for (String columnName : columnsDefinedOrder) {
            int width = widthEachColumn.get(columnName) + 5;
            widthEachColumn.put(columnName, width);
            tableWidth += width;
        }

        for (String columnName : columnsDefinedOrder) {
            sb.append(String.format("%-" + widthEachColumn.get(columnName) + "s", columnName));
        }
        sb.append(System.lineSeparator());

        for (int i = 0; i < tableWidth; i++) {
            sb.append("-");
        }
        sb.append(System.lineSeparator());

        for (Map<String, String> row : rows) {
            for (String columnName : columnsDefinedOrder) {
                sb.append(String.format("%-" + widthEachColumn.get(columnName) + "s", row.get(columnName)));
            }
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

    /**
     * assign the table to a string
     * @return a string that reprensent a table which can be printed
     */
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(System.lineSeparator());

        if (columnsDefinedOrder.size() <= 0) {
            columnsDefinedOrder = rows.stream()
                    .findFirst()
                    .map(x -> new ArrayList<>(x.keySet()))
                    .orElse(columnsDefinedOrder);
        }

        Map<String, Integer> widthEachColumn = new HashMap<>();
        for (String columnName : columnsDefinedOrder) {
            widthEachColumn.put(columnName, 0);
        }

        for (Map<String, String> row : rows) {
            for (String columnName : columnsDefinedOrder) {
                int length = row.get(columnName).length();
                if (length > widthEachColumn.get(columnName)) {
                    widthEachColumn.put(columnName, length);
                }
            }
        }

        for (String columnName : columnsDefinedOrder) {
            int width = widthEachColumn.get(columnName) + 5;
            widthEachColumn.put(columnName, width);
        }

        for (Map<String, String> row : rows) {
            for (String columnName : columnsDefinedOrder) {
                sb.append(String.format("%-" + widthEachColumn.get(columnName) + "s", row.get(columnName)));
            }
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

}
