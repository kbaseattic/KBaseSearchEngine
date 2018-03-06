package kbasesearchengine.search;

/** Thrown when an attempt to index and object in an indexing storage system results in a
 * conflict error. Storage systems that implement optimistic concurrency may throw these types
 * of errors.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class IndexingConflictException extends Exception {
    
    public IndexingConflictException(final String message, final Throwable exception) {
        super(message, exception);
    }
}