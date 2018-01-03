package kbasesearchengine.system;


/** The subtype of a {@link TransformType#location} transform.
 * This transform is specific to the KBase transform schema of [contig_id, start, strand, length].
 * @author gaprice@lbl.gov
 *
 */
public enum LocationTransformType {
	
    /** A transform that results in extracting the contig ID of the location. */
    contig_id,
    
    /** A transform that results in extracting the start of the location. */
    start,
    
    /** A transform that results in extracting the stop of the location. */
    stop,
    
    /** A transform that results in extracting the length of the location. */
    length,
    
    /** A transform that results in extracting the strand of the location. */
    strand;
    
}
