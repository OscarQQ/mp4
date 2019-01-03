package ca.ubc.ece.cpen221.mp4.db221.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.util.LinkedHashMap;
import java.util.Set;

public class QueryResult {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private final String status = "success";

    @SerializedName("table_name")
    private final String tableName = "query output";

    @SerializedName("table")
    private Set<LinkedHashMap<String, String>> rows;

    /**
     * QueryResult constructor
     * @param rows is a set of Linkedhashmaps that have Strings as keys and values.
     */
    public QueryResult(Set<LinkedHashMap<String, String>> rows) {
        this.rows = rows;
    }

    /** assign the QueryResult to a string
     *
     * @return a string that represents the Queryresult, most of the time it is a table
     */
    @Override
    public String toString() {
        return gson.toJson(this);
    }

}
