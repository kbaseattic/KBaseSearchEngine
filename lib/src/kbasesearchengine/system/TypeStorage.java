package kbasesearchengine.system;

import java.util.Set;

/** Stores a) type documents ({@link ObjectTypeParsingRules}) that describe how to transform an
 * object from a data source into one or more documents that can be indexed by search and
 * b) mappings ({@link TypeMapping}) from data source type versions ({@link StorageObjectType})
 * to search transformation document versions.
 * @author gaprice@lbl.gov
 *
 */
public interface TypeStorage {
    
    /** Returns a transformation document given a search transformation type and version.
     * @param type the search transformation type.
     * @return the transformation document.
     * @throws NoSuchTypeException if no such type exists.
     */
    ObjectTypeParsingRules getObjectTypeParsingRules(SearchObjectType type)
            throws NoSuchTypeException;

    /** Returns the latest version of all the search transformation types in the system.
     * @return the system types.
     */
    Set<ObjectTypeParsingRules> listObjectTypeParsingRules();
    
    /** Returns a set of search transformation specifications that apply to a given data source
     * storage object type. If there are no type mappings provided for the storage type, the
     * latest version of each search type is returned. Otherwise, the rules specified in the
     * type mappings are followed to determine which search type versions are returned.
     * 
     * @param storageObjectType the type of the data at the data storage system for which
     * search transformation specifications should be returned.
     * @return the transformation specifications or an empty set if no specifications are
     * available for the type.
     */
    Set<ObjectTypeParsingRules> listObjectTypeParsingRules(StorageObjectType storageObjectType);
}
