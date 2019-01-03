package ca.ubc.ece.cpen221.mp4.db221.handler;

import ca.ubc.ece.cpen221.mp4.db221.DB221;

import java.io.IOException;

public interface Handler {

    /**
     *Perform some operation correspond to the command
     * @param database the name of the database
     * @param command a string that represent an operation
     * @return a String that represents the table after the operation
     * @throws IOException when there's syntax errors or other errors
     */
    String handle(DB221 database, String command) throws IOException;

}
