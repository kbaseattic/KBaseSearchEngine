package kbaserelationengine.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.common.GUID;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.system.IndexingRules;

public interface IndexingStorage {
    
    /**
     * Adds object to searchable indexing storage.
     * @param id  global object ID including: storage type, parent object reference, inner sub-type and inner sub-object unique key (last two are optional)
     * @param objectType  global type of object
     * @param jsonValue  object value (consisting of maps, lists and primitive types like number, string, boolean or null)
     * @param indexingRules  indexing rules
     * @throws IOException
     */
    public void indexObject(GUID guid, String objectType, String jsonValue, String objectName,
            long timestamp, String parentJsonValue, Map<String, String> metadata, boolean isPublic,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException;

    public void indexObjects(String objectType, String objectName, long timestamp,
            String parentJsonValue, Map<String, String> metadata, Map<GUID, String> idToJsonValues,
            boolean isPublic, List<IndexingRules> indexingRules) 
                    throws IOException, ObjectParseException;
    
    public void flushIndexing(String objectType) throws IOException;
    
    public void shareObjects(Set<GUID> guids, int accessGroupId) throws IOException;

    public void unshareObjects(Set<GUID> guids, int accessGroupId) throws IOException;

    public void publishObjects(Set<GUID> guids) throws IOException;

    public void unpublishObjects(Set<GUID> guids) throws IOException;

    public List<ObjectData> getObjectsByIds(Set<GUID> guids) throws IOException;

    public Map<String, Integer> searchTypeByText(String text,
            AccessFilter accessFilter) throws IOException;

    public Set<GUID> searchIdsByText(String objectType, String text, List<SortingRule> sorting,
            AccessFilter accessFilter) throws IOException;

    public Set<GUID> lookupIdsByKey(String objectType, String keyName, Object keyValue,
            AccessFilter accessFilter) throws IOException;
}
