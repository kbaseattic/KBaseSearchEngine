package kbasesearchengine.events;

/** The processing state of a status event.
 * @author gaprice@lbl.gov
 *
 */
public enum StatusEventProcessingState {
    
    /** Not processed */
    UNPROC,
    
    /** Ready for processing */
    READY,
    
    /** Processing */
    PROC,
    
    /** Failed */
    FAIL,
    
    /** Processed but not indexed */
    UNINDX,
    
    /** indexed */
    INDX;
}
