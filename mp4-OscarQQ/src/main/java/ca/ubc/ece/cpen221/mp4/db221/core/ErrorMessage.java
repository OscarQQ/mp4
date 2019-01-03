package ca.ubc.ece.cpen221.mp4.db221.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ErrorMessage {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private final String status = "failure";
    private String message;

    /**
     * ErrorMessage constructor
     * @param message is a string
     */
    public ErrorMessage(String message) {
        this.message = message;
    }

    /** assign the error message to a string
     *
     * @return a string that represents the error message
     */
    @Override
    public String toString() {
        return gson.toJson(this);
    }

}
