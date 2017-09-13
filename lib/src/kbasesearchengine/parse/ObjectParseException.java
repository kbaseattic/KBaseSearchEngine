package kbasesearchengine.parse;

public class ObjectParseException extends Exception {
	private static final long serialVersionUID = 1L;

	public ObjectParseException() {
	}

	public ObjectParseException(String message) {
		super(message);
	}

	public ObjectParseException(Throwable cause) {
		super(cause);
	}

    public ObjectParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
