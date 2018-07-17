package kbasesearchengine.common;


/** An error thrown when the character length of a GUID is too long.
 *
 * Created by apasha on 5/30/18.
 */
public class GUIDTooLongException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public GUIDTooLongException(final String message) {
        super(message);
    }
}



