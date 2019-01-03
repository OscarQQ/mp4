package ca.ubc.ece.cpen221.mp4.db221;

import ca.ubc.ece.cpen221.mp4.db221.core.ErrorMessage;
import ca.ubc.ece.cpen221.mp4.db221.core.QueryResult;
import ca.ubc.ece.cpen221.mp4.db221.core.Table;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.BetweenColumnsCondition;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.ColumnLiteralCondition;
import ca.ubc.ece.cpen221.mp4.db221.core.condition.Condition;
import ca.ubc.ece.cpen221.mp4.db221.handler.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.*;

public class DB221Tests {

    @Test
    public void tablePrint() {
        Table table = new Table("test", Arrays.asList("col1", "col2", "col3"));
        table.columns();
        table.print();
    }

    @Test
    public void tableToString1() {
        Table table = new Table("test", Arrays.asList("col1", "col2", "col3"));
        table.columns();
        table.toString();
    }

    @Test
    public void getTable() throws IOException {
        DB221 test = new DB221("test");
        test.getTable("A");
    }

    @Test
    public void tableExistsFalse() throws IOException {
        DB221 test = new DB221("test");
        Assert.assertFalse(test.tableExists("books"));
    }

    @Test
    public void tableExistsTrue() throws IOException {
        DB221 test = new DB221("test", Collections.singletonList("books.json"));
        Assert.assertTrue(test.tableExists("books"));
    }

    @Test
    public void readDataFileFileNotFound() throws IOException {
        DB221 test = new DB221("test");
        boolean exception = false;
        try {
            test.readDataFile("not_exists.json");
        } catch (IOException ex) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

    @Test
    public void columns() {
        Table table = new Table("A", Arrays.asList("col1", "col2", "col3"));
        Set<String> columns = table.columns();
        Assert.assertNotNull(columns);
        Assert.assertEquals(3, columns.size());
    }

    @Test
    public void containsColumn() {
        Table table = new Table("A", Arrays.asList("col1", "col2", "col3"));
        Assert.assertTrue(table.containsColumn("col3"));
        Assert.assertFalse(table.containsColumn("col4"));
    }

    @Test
    public void fillColumnsDefinedOrder1() {
        Table table = new Table();
        Assert.assertFalse(table.containsColumn("col4"));
    }

    @Test
    public void fillColumnsDefinedOrder2() {
        Table table = new Table();
        Assert.assertEquals(0, table.columns().size());
    }

    @Test
    public void tableToString() throws IOException {
        DB221 test = new DB221("test", Collections.singletonList("books.json"));
        String jsonOutput = test.getTable("books").toString();
        System.out.println("table.toString(): " + System.lineSeparator() + jsonOutput);
    }

    @Test
    public void createHandlerNormal() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new CreateHandler();
        String resultMessage = handler.handle(test, "create table A ( a, b, c );");
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Set<LinkedHashMap<String, String>> emptyTable = new HashSet<>();
        String expected = gson.toJson(new QueryResult(emptyTable));
        Assert.assertEquals(expected, resultMessage);
    }

    @Test
    public void createHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new CreateHandler();
        String resultMessage = handler.handle(test, "create error");
        ErrorMessage errorMessage = new ErrorMessage("Syntax error, please check your statement");
        Assert.assertEquals(errorMessage.toString(), resultMessage);
    }

    @Test
    public void createHandlerDuplicateColumns() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new CreateHandler();
        String resultMessage = handler.handle(test, "create table A ( a, a, b );");
        ErrorMessage errorMessage = new ErrorMessage("There must not be any duplicate column names");
        Assert.assertEquals(errorMessage.toString(), resultMessage);
    }

    @Test
    public void printHandlerNormal() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new PrintHandler();
        String resultMessage = handler.handle(test, "print books;");
        Table table = test.readDataFile("books.json");
        Assert.assertEquals(table.print(), resultMessage);
    }

    @Test
    public void printHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new PrintHandler();
        String resultMessage = handler.handle(test, "print ,,,;");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void printHandlerTableNotExists() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new PrintHandler();
        String resultMessage = handler.handle(test, "print A;");
        Assert.assertEquals(new ErrorMessage("Table \"A\" does not exist").toString(), resultMessage);
    }

    @Test
    public void loadHandlerNormal() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new LoadHandler();
        String resultMessage = handler.handle(test, "load books;");
        Table table = test.readDataFile("books.json");
        Assert.assertEquals(new QueryResult(table.getRows()).toString(), resultMessage);
    }

    @Test
    public void loadHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new LoadHandler();
        String resultMessage = handler.handle(test, "load ...;");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void loadHandlerFileNotFound() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new LoadHandler();
        String NNUULLLL=null;
        String resultMessage = handler.handle(test, "load readers;");
        String resultMessage2 = handler.handle(test, "load  ;");
        Assert.assertEquals(new ErrorMessage("File \"readers.json\" not Found").toString(), resultMessage);
    }

    @Test
    public void storeHandlerNormal() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new StoreHandler();
        String resultMessage = handler.handle(test, "store books;");
        Table table = test.readDataFile("books.json");
        Assert.assertEquals(new QueryResult(table.getRows()).toString(), resultMessage);
    }

    @Test
    public void storeHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new StoreHandler();
        String resultMessage = handler.handle(test, "store ,,,;");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void storeHandlerTableNotExists() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new StoreHandler();
        String resultMessage = handler.handle(test, "store A;");
        Assert.assertEquals(new ErrorMessage("Table \"A\" does not exist").toString(), resultMessage);
    }

    @Test
    public void storeHandlerCreateFile() throws IOException {
        DB221 test = new DB221("test");
        Table table = new Table("TA", Arrays.asList("col1", "col2", "col3"));
        test.newTable(table);
        Handler handler = new StoreHandler();
        String resultMessage = handler.handle(test, "store TA;");
        Assert.assertEquals(new QueryResult(table.getRows()).toString(), resultMessage);
    }

    @Test
    public void storeHandlerJsonHelper() {
        StoreHandler handler = new StoreHandler();
        Gson gson = handler.getJsonHelper();
        Assert.assertNotNull(gson);
    }

    @Test
    public void insertHandlerNormal() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new InsertHandler();
        String resultMessage = handler.handle(test, "insert into books values ( 0213479, book, george );");
        Table table = test.readDataFile("books.json");
        table.insert(new String[] {"0213479", "book", "george"});
        Assert.assertEquals(new QueryResult(table.getRows()).toString(), resultMessage);
    }

    @Test
    public void insertHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new InsertHandler();
        String resultMessage = handler.handle(test, "insert ...';");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void insertHandlerTableNotExists() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new InsertHandler();
        String resultMessage = handler.handle(test, "insert into A values ( a, b, c );");
        Assert.assertEquals(new ErrorMessage("Table \"A\" does not exist").toString(), resultMessage);
    }

    @Test
    public void insertHandlerColumnNotMatches() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new InsertHandler();
        String resultMessage = handler.handle(test, "insert into books values ( 1992, 1992, 1992, 1992 );");
        Assert.assertEquals(new ErrorMessage("Columns do not match").toString(), resultMessage);
    }

    @Test
    public void interactiveNoArgs() throws IOException {
        PrintStream stdErr = System.err;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        System.setErr(new PrintStream(os));
        DB221.main(new String[] {});
        String errorMessage = os.toString();
        System.setErr(stdErr);
        Assert.assertEquals("Please input the name of the database" + System.lineSeparator(), errorMessage);
    }

    @Test
    public void interactiveDBNameAndTables() throws IOException {
        InputStream stdIn = System.in;
        PrintStream stdErr = System.out;
        try {
            String input = "create table A ( a, b, c );" + System.lineSeparator()
                    + "insert into table A values ( x, y, z );" + System.lineSeparator()
                    + "load books;" + System.lineSeparator()
                    + "print books;" + System.lineSeparator()
                    + "select isbn from books where author >= \"jane austen\";" + System.lineSeparator()
                    + "store A;" + System.lineSeparator()
                    + "alter A;" + System.lineSeparator()
                    + "    " + System.lineSeparator()
                    + "exit;" + System.lineSeparator();
            ByteArrayInputStream is = new ByteArrayInputStream(input.getBytes());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            System.setIn(is);
            System.setErr(new PrintStream(os));
            DB221.main(new String[]{"test", "books.json", "publishers.json"});
            Assert.assertEquals("Unsupported Operation: \"alter\", please check your statement.", os.toString().trim());
        } catch (Exception ex) {

        } finally {
            System.setIn(stdIn);
            System.setErr(stdErr);
        }
    }

    @Test
    public void interactiveEOF() {
        InputStream stdIn = System.in;
        PrintStream stdErr = System.out;
        try {
            String input = "";
            ByteArrayInputStream is = new ByteArrayInputStream(input.getBytes());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            System.setIn(is);
            System.setErr(new PrintStream(os));
            DB221.main(new String[]{"test", "books.json", "publishers.json"});
            Assert.assertEquals("", os.toString().trim());
        } catch (Exception ex) {

        } finally {
            System.setIn(stdIn);
            System.setErr(stdErr);
        }
    }

    @Test
    public void interactiveFileNotFound() {
        InputStream stdIn = System.in;
        PrintStream stdErr = System.out;
        try {
            String input = "";
            ByteArrayInputStream is = new ByteArrayInputStream(input.getBytes());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            System.setIn(is);
            System.setErr(new PrintStream(os));
            DB221.main(new String[]{"test", "ABC.json"});
            Assert.assertEquals("File ABC.json not found. ", os.toString().trim());
        } catch (Exception ex) {

        } finally {
            System.setIn(stdIn);
            System.setErr(stdErr);
        }
    }

    @Test
    public void snapshotDirNotExists() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        test.snapshot();
    }

    @Test
    public void selectHandlerSyntaxError() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select ...;");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void selectHandlerMoreThanTwoTables() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select a, b from A, B, C;");
        Assert.assertEquals(new ErrorMessage("Please check your statement").toString(), resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesNoCondition() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select book_title, publisher from books, publishers;");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesNoCommonColumns() throws IOException {
        DB221 test = new DB221("test");
        Table tableA = new Table("A", Arrays.asList("a", "b", "c"));
        Table tableB = new Table("B", Arrays.asList("d", "e", "f"));
        test.newTable(tableA);
        test.newTable(tableB);
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select a, e from A, B;");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesFirstTableNotExists() throws IOException {
        DB221 test = new DB221("test");
        Table tableB = new Table("B", Arrays.asList("d", "e", "f"));
        test.newTable(tableB);
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select a, e from A, B;");
        Assert.assertEquals(new ErrorMessage("Table A does not exist").toString(), resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesSecondTableNotExists() throws IOException {
        DB221 test = new DB221("test");
        Table tableA = new Table("A", Arrays.asList("a", "b", "c"));
        test.newTable(tableA);
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select a, e from A, B;");
        Assert.assertEquals(new ErrorMessage("Table B does not exist").toString(), resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesColumnNotExists() throws IOException {
        DB221 test = new DB221("test");
        Table tableA = new Table("A", Arrays.asList("a", "b", "c"));
        Table tableB = new Table("B", Arrays.asList("d", "e", "f"));
        tableA.insert(new String[] {"aval1", "bval1", "cval1"});
        tableA.insert(new String[] {"aval2", "bval2", "cval2"});
        tableB.insert(new String[] {"dval1", "eval1", "fval1"});
        tableB.insert(new String[] {"dval2", "eval2", "fval2"});
        test.newTable(tableA);
        test.newTable(tableB);
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select a, x from A, B;");
        Assert.assertEquals(new ErrorMessage("Table A JOIN B does not have column \"x\"").toString(), resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesWithConditions() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select book_title, publisher from books, publishers where year >= \"1992\" and isbn = \"3\" or author > publisher;");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerSingleTableWithConditions2() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select isbn from publishers where isbn=\"2522309\" and year >= \"1990\";");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerTwoTablesWithConditions2() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select book_title, publisher from books, publishers where year >= \"1992\" and isbn = \"2522309\" or author > publisher;");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerSingleTableNoCondition() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select isbn from publishers;");
        System.out.println(resultMessage);
    }

    @Test
    public void selectHandlerSingleTableColumnNotExists() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select xyz from publishers;");
        Assert.assertEquals(new ErrorMessage("Table \"publishers\" does not have column \"xyz\"").toString(), resultMessage);
    }

    @Test
    public void selectHandlerSingleTableColumnNotExistsWithConditions() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select xyz from publishers where isbn > \"0123456\";");
        Assert.assertEquals(new ErrorMessage("Table \"publishers\" does not have column \"xyz\"").toString(), resultMessage);
    }

    @Test
    public void selectHandlerSingleTableNotExists() throws IOException {
        DB221 test = new DB221("test");
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select xyz from A;");
        Assert.assertEquals(new ErrorMessage("Table \"A\" does not exist").toString(), resultMessage);
    }


    @Test
    public void selectHandlerSingleTableWithConditions() throws IOException {
        DB221 test = new DB221("test", Arrays.asList("books.json", "publishers.json"));
        Handler handler = new SelectHandler();
        String resultMessage = handler.handle(test, "select isbn from publishers where publisher != \"penguin\" and year < \"1984\" or isbn <= \"2522309\";");
        System.out.println(resultMessage);
    }

    @Test
    public void conditionToString1() {
        Condition cond1 = new BetweenColumnsCondition("<", "a", "b");
        Assert.assertEquals("a < b", cond1.toString());
    }

    @Test
    public void conditionToString2() {
        Condition cond1 = new ColumnLiteralCondition("<", "a", "4");
        Assert.assertEquals("a < \"4\"", cond1.toString());
    }

    @Test
    public void betweenCondition() {
        Map<String, String> rows1 = new HashMap<>();
        Map<String, String> rows2 = new HashMap<>();
        rows1.put("a", "1");
        rows2.put("b", "2");
        Condition cond1 = new BetweenColumnsCondition("<", "a", "b");
        Condition cond2 = new BetweenColumnsCondition("<=", "a", "b");
        Condition cond3 = new BetweenColumnsCondition(">", "a", "b");
        Condition cond4 = new BetweenColumnsCondition(">=", "a", "b");
        Condition cond5 = new BetweenColumnsCondition("=", "a", "b");
        Condition cond6 = new BetweenColumnsCondition("!=", "a", "b");
        Condition cond7 = new BetweenColumnsCondition("<>", "", "");
        Assert.assertTrue(cond1.test(rows1, rows2));
        Assert.assertTrue(cond2.test(rows1, rows2));
        Assert.assertFalse(cond3.test(rows1, rows2));
        Assert.assertFalse(cond4.test(rows1, rows2));
        Assert.assertFalse(cond5.test(rows1, rows2));
        Assert.assertTrue(cond6.test(rows1, rows2));
        Assert.assertFalse(cond7.test(rows1, rows2));

        rows1.clear();
        rows2.clear();
        rows1.put("a", "2");
        rows2.put("b", "1");
        Assert.assertFalse(cond1.test(rows1, rows2));
        Assert.assertFalse(cond2.test(rows1, rows2));
        Assert.assertTrue(cond3.test(rows1, rows2));
        Assert.assertTrue(cond4.test(rows1, rows2));
        Assert.assertFalse(cond5.test(rows1, rows2));
        Assert.assertTrue(cond6.test(rows1, rows2));
        Assert.assertFalse(cond7.test(rows1, rows2));

        rows1.clear();
        rows2.clear();
        rows1.put("a", "2");
        rows2.put("b", "2");
        Assert.assertFalse(cond1.test(rows1, rows2));
        Assert.assertTrue(cond2.test(rows1, rows2));
        Assert.assertFalse(cond3.test(rows1, rows2));
        Assert.assertTrue(cond4.test(rows1, rows2));
        Assert.assertTrue(cond5.test(rows1, rows2));
        Assert.assertFalse(cond6.test(rows1, rows2));
        Assert.assertFalse(cond7.test(rows1, rows2));
    }

    @Test
    public void literalCondition() {
        Map<String, String> rows = new HashMap<>();
        Map<String, String> dummy = new HashMap<>();
        rows.put("a", "1");
        Condition cond1 = new ColumnLiteralCondition("<", "a", "2");
        Condition cond2 = new ColumnLiteralCondition("<=", "a", "2");
        Condition cond3 = new ColumnLiteralCondition(">", "a", "2");
        Condition cond4 = new ColumnLiteralCondition(">=", "a", "2");
        Condition cond5 = new ColumnLiteralCondition("=", "a", "2");
        Condition cond6 = new ColumnLiteralCondition("!=", "a", "2");
        Condition cond7 = new ColumnLiteralCondition("<>", "", "");

        Assert.assertTrue(cond1.test(rows, dummy));
        Assert.assertTrue(cond2.test(rows, dummy));
        Assert.assertFalse(cond3.test(rows, dummy));
        Assert.assertFalse(cond4.test(rows, dummy));
        Assert.assertFalse(cond5.test(rows, dummy));
        Assert.assertTrue(cond6.test(rows, dummy));
        Assert.assertFalse(cond7.test(rows, dummy));

        rows.clear();
        rows.put("a", "3");
        Assert.assertFalse(cond1.test(rows, dummy));
        Assert.assertFalse(cond2.test(rows, dummy));
        Assert.assertTrue(cond3.test(rows, dummy));
        Assert.assertTrue(cond4.test(rows, dummy));
        Assert.assertFalse(cond5.test(rows, dummy));
        Assert.assertTrue(cond6.test(rows, dummy));
        Assert.assertFalse(cond7.test(rows, dummy));

        rows.clear();
        rows.put("a", "2");
        Assert.assertFalse(cond1.test(rows, dummy));
        Assert.assertTrue(cond2.test(rows, dummy));
        Assert.assertFalse(cond3.test(rows, dummy));
        Assert.assertTrue(cond4.test(rows, dummy));
        Assert.assertTrue(cond5.test(rows, dummy));
        Assert.assertFalse(cond6.test(rows, dummy));
        Assert.assertFalse(cond7.test(rows, dummy));
    }

}
