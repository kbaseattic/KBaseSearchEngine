package kbasesearchengine.events.exceptions;


public class EmptyStringException extends Exception {

    private static final String MESSAGE = "Words can't be empty or just white spaces";
    public EmptyStringException() {
        super(MESSAGE);
    }
}
