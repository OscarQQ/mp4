package ca.ubc.ece.cpen221.mp4.db221.util;

import ca.ubc.ece.cpen221.mp4.db221.DB221;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class Constants {

    public static final String CREATE = "create";
    public static final String INSERT = "insert";
    public static final String LOAD = "load";
    public static final String PRINT = "print";
    public static final String SELECT = "select";
    public static final String STORE = "store";
    public static final Pattern EXIT_PATTERN = Pattern.compile("(quit\\s*;)|(exit\\s*;)");
    private static final String NAME_REGEX = "[a-zA-Z]+\\w*";
    public static final Pattern LOAD_PATTERN = Pattern.compile("load\\s+(" + NAME_REGEX + ")\\s*;");
    public static final Pattern PRINT_PATTERN = Pattern.compile("print\\s+(" + NAME_REGEX + ")\\s*;");
    public static final Pattern STORE_PATTERN = Pattern.compile("store\\s+(" + NAME_REGEX + ")\\s*;");
    private static final String TABLE_DEF_REGEX = ("\\(\\s*" + NAME_REGEX + "(\\s*,\\s*" + NAME_REGEX + ")*\\s*\\)");
    public static final Pattern CREATE_PATTERN = Pattern.compile("create\\s+table\\s+(" + NAME_REGEX + ")\\s*(" + TABLE_DEF_REGEX + ")\\s*;");
    private static final String LITERAL_REGEX = "[^,\\s]+";
    public static final String VALUE_REGEX = "\\(\\s*" + LITERAL_REGEX + "(\\s*,\\s*" + LITERAL_REGEX + ")*\\s*\\)";
    private static final String VALUES_REGEX = VALUE_REGEX + "(\\s*,\\s*" + VALUE_REGEX + ")*";
    public static final Pattern INSERT_PATTERN = Pattern.compile("insert\\s+into\\s+(" + NAME_REGEX + ")\\s+values\\s*(" + VALUES_REGEX + ")\\s*;");
    private static final String COLUMNS_REGEX = NAME_REGEX + "(\\s*,\\s*" + NAME_REGEX + ")*";
    private static final String TABLES_REGEX = NAME_REGEX + "(\\s*,\\s*" + NAME_REGEX + ")?";
    private static final String RELATION_REGEX = "(<)|(>)|(=)|(!=)|(<=)|(>=)";
    public static final String CONDITION_REGEX = "(" + NAME_REGEX + ")\\s*(" + RELATION_REGEX + ")\\s*((" + NAME_REGEX + ")|(\"" + LITERAL_REGEX + "\"))";
    private static final String WHERE_REGEX = "\\s+where\\s+(" + CONDITION_REGEX + "(\\s+and\\s+" + CONDITION_REGEX + ")*(\\s+or\\s+" + CONDITION_REGEX + ")*)";
    public static final Pattern SELECT_PATTERN = Pattern.compile("select\\s+(" + COLUMNS_REGEX + ")\\s+from\\s+(" + TABLES_REGEX + ")(" + WHERE_REGEX + ")?\\s*;");

}
