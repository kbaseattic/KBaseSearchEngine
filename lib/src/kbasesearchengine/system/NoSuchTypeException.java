package kbasesearchengine.system;

/** Thrown when a requested type doesn't exist.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class NoSuchTypeException extends Exception {
    
    public NoSuchTypeException(final String message) {
        super(message);
    }
    
    public NoSuchTypeException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
