package kbaserelationengine.parse;

import java.io.IOException;
import java.util.Map;

public interface IndexingStorage {
    public void addObjectType(ObjectTypeParsingRules typeDef) throws IOException;
    
    public ObjectTypeParsingRules getObjectType(String globalObjectType) throws IOException;
    
    /**
     * Adds object to searchable indexing storage.
     * @param id  global object ID including: storage type, parent object reference, inner sub-type and inner sub-object unique key (last two are optional)
     * @param objectType  global type of object
     * @param typedId  unique ID across objects of given type including: parent object reference and inner sub-object unique key (last one is optional)
     * @param value  object value (consisting of maps, lists and primitive types like number, string, boolean or null)
     * @param indexingPathToRules  indexing rules
     * @throws IOException
     */
    public void indexObject(String id, String objectType, String typedId, Object value, 
            Map<ObjectJsonPath, IndexingRules> indexingPathToRules) throws IOException;
    
    public String lookupIdByTypedId(String objectType, String typedId) throws IOException;
}
