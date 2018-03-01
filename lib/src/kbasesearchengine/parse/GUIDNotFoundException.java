package kbasesearchengine.parse;

/** An error thrown when a GUID was expected to be found in a set of GUIDs but was not.
 * @author gaprice@lbl.gov
 *
 */
public class GUIDNotFoundException extends ObjectParseException {

    private static final long serialVersionUID = 1L;
    
    public GUIDNotFoundException(final String message) {
        super(message);
    }

}
