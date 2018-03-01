package kbasesearchengine.events.exceptions;

/** An enum representing the type of a particular error.
 * @author gaprice@lbl.gov
 *
 */
public enum ErrorType {

    // be very careful about changing error type ids as they may be stored in DBs
    
    /* add new error types as needed. These are primarily to allow selecting or excluding
     * types of errors when doing database searches.
     */

    /** Lack of location data for a location transform. */
    LOCATION_ERROR,
    
    /** An object has too many subobjects to index. */
    SUBOBJECT_COUNT,
    
    /** A conflict error when attempting to index data. */
    INDEXING_CONFLICT,
    
    /** The object was deleted in the source data storage system and could not be retrieved. */
    DELETED,
    
    /** A GUID was expected, but was not available. */
    GUID_NOT_FOUND,

    /** A catch all category for error types without a specific entry. */
    OTHER;
    
    


}
