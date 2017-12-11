package kbasesearchengine.system;

/** Thrown when an exception occurs when parsing type mapping or type transform specs.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class TypeParseException extends Exception {
    
    public TypeParseException(final String message) {
        super(message);
    }
    
    public TypeParseException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
