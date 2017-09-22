package kbasesearchengine.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.system.IndexingRules;

public interface IndexingStorage {
    
    /**
     * Adds object to searchable indexing storage.
     * @param id  global object ID including: storage type, parent object reference, inner sub-type and inner sub-object unique key (last two are optional)
     * @param objectType  global type of object
     * @param jsonValue  object value (consisting of maps, lists and primitive types like number, string, boolean or null)
     * @param indexingRules  indexing rules
     * @throws IOException
     */
    public void indexObject(GUID guid, String objectType, ParsedObject obj, SourceData objectName,
            long timestamp, String parentJsonValue, boolean isPublic,
            List<IndexingRules> indexingRules) throws IOException;

    public void indexObjects(String objectType, SourceData obj, long timestamp,
            String parentJsonValue, GUID pguid, Map<GUID, ParsedObject> idToObj,
            boolean isPublic, List<IndexingRules> indexingRules) 
                    throws IOException;
    
    public Map<GUID, Boolean> checkParentGuidsExist(String objectType, Set<GUID> parentGuids) 
            throws IOException;

    public void flushIndexing(String objectType) throws IOException;
    
    public void shareObjects(Set<GUID> guids, int accessGroupId, boolean isPublicGroup) throws IOException;

    public void unshareObjects(Set<GUID> guids, int accessGroupId) throws IOException;

    public void publishObjects(Set<GUID> guids) throws IOException;

    public void unpublishObjects(Set<GUID> guids) throws IOException;

    public void publishObjectsExternally(Set<GUID> guids, int accessGroupId) throws IOException;

    public void unpublishObjectsExternally(Set<GUID> guids, int accessGroupId) throws IOException;

    public List<ObjectData> getObjectsByIds(Set<GUID> guids, PostProcessing postProcessing) 
            throws IOException;

    public Map<String, Integer> searchTypes(MatchFilter matchFilter,
            AccessFilter accessFilter) throws IOException;

    public FoundHits searchIds(String objectType, MatchFilter matchFilter, 
            List<SortingRule> sorting, AccessFilter accessFilter, Pagination pagination) 
                    throws IOException;

    public FoundHits searchObjects(String objectType, MatchFilter matchFilter, 
            List<SortingRule> sorting, AccessFilter accessFilter, Pagination pagination,
            PostProcessing postProcessing) throws IOException;

    /** Change the name of all the versions of an object.
     * @param object the GUID of the object. The version field is ignored.
     * @param newName the new name of the object.
     * @return the number of documents modified, including sub objects.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     */
    int setNameOnAllObjectVersions(GUID object, String newName) throws IOException;

    /** Delete all versions of an object from its access group. The object may still be
     * accessible via other access groups.
     * @param guid the object to delete.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     */
    void deleteAllVersions(GUID guid) throws IOException;

    /** Delete all versions of an object from its access group.
     * @param guid the object to delete.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     */
    void undeleteAllVersions(GUID guid) throws IOException;

    /** Set all versions of an object to public.
     * @param guid the object to publish.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     */
    void publishAllVersions(GUID guid) throws IOException;

    /** Make all versions of an object private.
     * @param guid the object to make private.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     */
    void unpublishAllVersions(GUID guid) throws IOException;
}
