package kbasesearchengine.system;

/** The type of a transform specified in an {@link IndexingRules}.
 * @author gaprice@lbl.gov
 *
 */
public enum TransformType {
    
    /** Transforms KBase location tuple ([contig_id, start, strand, length]) to one of
     *  "contig_id", "start", "strand", "length" or "stop" values corresponding to the
     *  accompanying transformation property, e.g. location.strand.
     */
    location,
    
    /** Transforms object or array of values to strings of all primitive components. */
    values,
    
    /** Transforms value to string (String.valueOf function). */
    string,
    
    /** Transforms value to Integer (Integer.parseInt of String.valueOf). */
    integer,
    
    /** transforms value containing reference to data store object into GUID.
     */
    guid,
    
    /** transforms value containing GUID into keyword loaded from content of external object
     *  referenced by said GUID. External keyword name is defined in accompanying transformation
     *  property, e.g. lookup.mykeyofinterest.
     */
    lookup;
}

