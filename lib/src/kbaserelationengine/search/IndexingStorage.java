package kbaserelationengine.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.system.IndexingRules;

public interface IndexingStorage {
    
    /**
     * Adds object to searchable indexing storage.
     * @param id  global object ID including: storage type, parent object reference, inner sub-type and inner sub-object unique key (last two are optional)
     * @param objectType  global type of object
     * @param typedId  unique ID across objects of given type including: parent object reference and inner sub-object unique key (last one is optional)
     * @param value  object value (consisting of maps, lists and primitive types like number, string, boolean or null)
     * @param indexingPathToRules  indexing rules
     * @throws IOException
     */
    public void indexObject(GUID id, String objectType, Object value, 
            Map<ObjectJsonPath, IndexingRules> indexingPathToRules) throws IOException;
    
    public List<Object> getObjectsByIds(Set<GUID> id) throws IOException;
    
    public Set<GUID> searchIdsByText(String text, List<SortingRule> sorting) throws IOException;

    public Set<GUID> lookupIdsByKey(String keyName, Object keyValue) throws IOException;
}
