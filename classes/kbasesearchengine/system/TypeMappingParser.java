package kbasesearchengine.system;

import java.io.InputStream;
import java.util.Set;

/** A parser for some source of storage type to search type mappings.
 * 
 * @author gaprice@lbl.gov
 *
 */
public interface TypeMappingParser {

    /** Get the type mappings found by the parser from some intput.
     * @param input the input.
     * @param sourceInfo information about the source of the input, often a file name.
     * @return the type mappings.
     * @throws TypeParseException if the type mapping could not be parsed.
     */
    Set<TypeMapping> parse(InputStream input, String sourceInfo) throws TypeParseException;
    
}
