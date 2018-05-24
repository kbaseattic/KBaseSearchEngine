package kbasesearchengine.search;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.system.ObjectTypeParsingRules;

public interface IndexingStorage {
    
    /** Drop all data from the storage system. 
     * @throws IOException if an IO error occcurs.
     */
    public void dropData() throws IOException;
    
    /**
     * Adds object to searchable indexing storage.
     * @param id  global object ID including: storage type, parent object reference,
     * inner sub-type and inner sub-object unique key (last two are optional)
     * @param objectType  global type of object
     * @param jsonValue  object value (consisting of maps, lists and primitive types like
     * number, string, boolean or null)
     * @param indexingRules  indexing rules
     * @throws IOException
     * @throws IndexingConflictException if a conflict occurs while modifying the index. 
     */
    public void indexObject(
            ObjectTypeParsingRules rule,
            SourceData source,
            Instant timestamp,
            String parentJsonValue,
            GUID guid,
            ParsedObject obj)
            throws IOException, IndexingConflictException;

    public void indexObjects(
            ObjectTypeParsingRules rule,
            SourceData source,
            Instant timestamp,
            String parentJsonValue,
            GUID pguid,
            Map<GUID, ParsedObject> idToObj)
            throws IOException, IndexingConflictException;
    
    /** Check that the parent objects (e.g. the access information) exists for a set of GUIDS.
     * Equivalent to {@link #checkParentGuidsExist(String, Set)} with a null String.
     * @param parentGuids the parent guids to check.
     * @return a map from GUID to the existence of the parent document for the GUID.
     * @throws IOException if an IO error occurs contacting the storage system.
     */
    public Map<GUID, Boolean> checkParentGuidsExist(Set<GUID> parentGuids)
            throws IOException;

    public void flushIndexing(ObjectTypeParsingRules objectType) throws IOException;
    
    public void shareObjects(Set<GUID> guids, int accessGroupId, boolean isPublicGroup)
            throws IOException, IndexingConflictException;

    public void unshareObjects(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException;

    public void publishObjects(Set<GUID> guids) throws IOException, IndexingConflictException;

    public void unpublishObjects(Set<GUID> guids) throws IOException, IndexingConflictException;

    public void publishObjectsExternally(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException;

    public void unpublishObjectsExternally(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException;

    public List<ObjectData> getObjectsByIds(Set<GUID> guids) throws IOException;
    
    public List<ObjectData> getObjectsByIds(Set<GUID> guids, PostProcessing postProcessing) 
            throws IOException;

    public Map<String, Integer> searchTypes(MatchFilter matchFilter,
            AccessFilter accessFilter) throws IOException;

    /**
    *
    * @param objectType a non-null list of object types to constrain the search to.
    *                   An empty list indicates a search that is unconstrained by object type.
    * @param matchFilter
    * @param sorting
    * @param accessFilter
    * @param pagination
    * @return
    * @throws IOException
    */
    public FoundHits searchIds(
            List<String> objectType,
            MatchFilter matchFilter,
            List<SortingRule> sorting,
            AccessFilter accessFilter,
            Pagination pagination)
            throws IOException;

    /**
    *
    * @param objectType a non-null list of object types to constrain the search to.
    *                   An empty list indicates a search that is unconstrained by object type.
    * @param matchFilter
    * @param sorting
    * @param accessFilter
    * @param pagination
    * @param postProcessing
    * @return
    * @throws IOException
    */
    public FoundHits searchObjects(
            List<String> objectType,
            MatchFilter matchFilter,
            List<SortingRule> sorting,
            AccessFilter accessFilter,
            Pagination pagination,
            PostProcessing postProcessing)
            throws IOException;

    /** Change the name of all the versions of an object.
     * @param object the GUID of the object. The version field is ignored.
     * @param newName the new name of the object.
     * @return the number of documents modified, including sub objects.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     * @throws IndexingConflictException if a conflict occurs while modifying the index. 
     */
    int setNameOnAllObjectVersions(GUID object, String newName)
            throws IOException, IndexingConflictException;

    /** Delete all versions of an object from its access group. The object may still be
     * accessible via other access groups.
     * @param guid the object to delete.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     * @throws IndexingConflictException if a conflict occurs while modifying the index.
     */
    void deleteAllVersions(GUID guid) throws IOException, IndexingConflictException;

    /** Delete all versions of an object from its access group.
     * @param guid the object to delete.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     * @throws IndexingConflictException if a conflict occurs while modifying the index. 
     */
    void undeleteAllVersions(GUID guid) throws IOException, IndexingConflictException;

    /** Set all versions of an object to public.
     * @param guid the object to publish.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     * @throws IndexingConflictException if a conflict occurs while modifying the index. 
     */
    void publishAllVersions(GUID guid) throws IOException, IndexingConflictException;

    /** Make all versions of an object private.
     * @param guid the object to make private.
     * @throws IOException if an IO error occurs when contacting the indexing storage.
     * @throws IndexingConflictException if a conflict occurs while modifying the index. 
     */
    void unpublishAllVersions(GUID guid) throws IOException, IndexingConflictException;
}
