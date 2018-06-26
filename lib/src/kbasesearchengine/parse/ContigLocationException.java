package kbasesearchengine.parse;

/** An error thrown when data regarding locations on a contig are missing or erroneous.
 * @author gaprice@lbl.gov
 *
 */
public class ContigLocationException extends ObjectParseException {

    private static final long serialVersionUID = 1L;
    
    public ContigLocationException(final String message) {
        super(message);
    }

}
