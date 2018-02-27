package kbasesearchengine.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

//TODO CODE remove 'fake' group IDs (-1 public, -2 admin). use alternate mechanism.

public class ElasticIndexingStorage implements IndexingStorage {

    private static final String SUBTYPE_INDEX_SUFFIX = "_sub";
    private static final String EXCLUDE_SUB_OJBS_URL_SUFFIX = ",-*" + SUBTYPE_INDEX_SUFFIX;
    private static final String OBJ_GUID = "guid";
    private static final String OBJ_TIMESTAMP = "timestamp";
    private static final String OBJ_PROV_COMMIT_HASH = "prv_cmt";
    private static final String OBJ_PROV_MODULE_VERSION = "prv_ver";
    private static final String OBJ_PROV_METHOD = "prv_meth";
    private static final String OBJ_PROV_MODULE = "prv_mod";
    private static final String OBJ_MD5 = "md5";
    private static final String OBJ_COPIER = "copier";
    private static final String OBJ_CREATOR = "creator";
    private static final String OBJ_NAME = "oname";
    private static final String OBJ_PREFIX = "prefix";
    private static final String OBJ_STORAGE_CODE = "str_cde";
    private static final String OBJ_ACCESS_GROUP_ID = "accgrp";
    private static final String OBJ_VERSION = "version";
    private static final String OBJ_IS_LAST = "islast";
    private static final String OBJ_PUBLIC = "public";
    private static final String OBJ_SHARED = "shared";

    // tags on the data originating at the source of the data
    private static final String SOURCE_TAGS = "stags";
    
    private static final String SEARCH_OBJ_TYPE = "otype";
    private static final String SEARCH_OBJ_TYPE_VER = "otypever";

    //readable names
    private static final String R_OBJ_GUID = "guid";
    private static final String R_OBJ_TIMESTAMP = "timestamp";
    private static final String R_OBJ_PROV_COMMIT_HASH = "provenance_commit";
    private static final String R_OBJ_PROV_MODULE_VERSION = "provenance_module_ver";
    private static final String R_OBJ_PROV_METHOD = "provenance_method";
    private static final String R_OBJ_PROV_MODULE = "provenance_module";
    private static final String R_OBJ_MD5 = "md5";
    private static final String R_OBJ_COPIER = "copier";
    private static final String R_OBJ_CREATOR = "creator";
    private static final String R_OBJ_NAME = "object_name";
    private static final String R_OBJ_PREFIX = "guid_prefix";
    private static final String R_OBJ_STORAGE_CODE = "storage_code";
    private static final String R_OBJ_ACCESS_GROUP_ID = "access_group_id";
    private static final String R_OBJ_VERSION = "version";
    private static final String R_OBJ_IS_LAST = "is_last_version";
    private static final String R_OBJ_PUBLIC = "is_public";
    private static final String R_OBJ_SHARED = "is_shared";

    // tags on the data originating at the source of the data
    private static final String R_SOURCE_TAGS = "source_tags";

    private static final String R_SEARCH_OBJ_TYPE = "type";
    private static final String R_SEARCH_OBJ_TYPE_VER = "type_ver";

    private static final ImmutableBiMap<String, String> READABLE_NAMES = ImmutableBiMap.
            <String,String>builder()
            .put(OBJ_GUID, R_OBJ_GUID)
            .put(OBJ_TIMESTAMP, R_OBJ_TIMESTAMP)
            .put(OBJ_PROV_COMMIT_HASH, R_OBJ_PROV_COMMIT_HASH)
            .put(OBJ_PROV_MODULE_VERSION, R_OBJ_PROV_MODULE_VERSION)
            .put(OBJ_PROV_METHOD, R_OBJ_PROV_METHOD)
            .put(OBJ_PROV_MODULE, R_OBJ_PROV_MODULE)
            .put(OBJ_MD5, R_OBJ_MD5)
            .put(OBJ_COPIER, R_OBJ_COPIER)
            .put(OBJ_CREATOR, R_OBJ_CREATOR)
            .put(OBJ_NAME, R_OBJ_NAME)
            .put(SOURCE_TAGS, R_SOURCE_TAGS)
            .put(SEARCH_OBJ_TYPE, R_SEARCH_OBJ_TYPE)
            .put(SEARCH_OBJ_TYPE_VER, R_SEARCH_OBJ_TYPE_VER)
            .put(OBJ_PREFIX, R_OBJ_PREFIX)
            .put(OBJ_STORAGE_CODE, R_OBJ_STORAGE_CODE)
            .put(OBJ_ACCESS_GROUP_ID, R_OBJ_ACCESS_GROUP_ID)
            .put(OBJ_VERSION, R_OBJ_VERSION)
            .put(OBJ_IS_LAST, R_OBJ_IS_LAST)
            .put(OBJ_PUBLIC, R_OBJ_PUBLIC)
            .put(OBJ_SHARED, R_OBJ_SHARED)
            .build();

    private HttpHost esHost;
    private String esUser;
    private String esPassword;
    private String indexNamePrefix;
    private Map<ObjectTypeParsingRules, String> ruleToIndex = new LinkedHashMap<>();
    private Map<String, String> typeToIndex = new LinkedHashMap<>();
    private RestClient restClient = null;
    private File tempDir;
    
    public static final int PUBLIC_ACCESS_GROUP = -1;
    public static final int ADMIN_ACCESS_GROUP = -2;

    /** Maximum number of types that can be fit into the ElasticSearch HTTP URL max
     * length of 4kb (default).
     * i.e., MAX_OBJECT_TYPES_SIZE * {@link SearchObjectType#MAX_TYPE_SIZE} << URL length
     * giving us room for other url attribs like blacklists and subtype search filtering.
     *
     * This value is also reflected in the limit specified for SearchObjectsInput.object_types in
     * KBaseSearchEngine.spec.
     *
     */
    public static final int MAX_OBJECT_TYPES_SIZE = 50;

    public ElasticIndexingStorage(HttpHost esHost, File tempDir) throws IOException {
        this.esHost = esHost;
        this.indexNamePrefix = "";
        this.tempDir = tempDir;
    }
    
    public HttpHost getEsHost() {
        return esHost;
    }
    
    public File getTempDir() {
        return tempDir;
    }
    
    public String getEsUser() {
        return esUser;
    }
    
    public void setEsUser(String esUser) {
        this.esUser = esUser;
    }
    
    public void setEsPassword(String esPassword) {
        this.esPassword = esPassword;
    }

    public String getIndexNamePrefix() {
        return indexNamePrefix;
    }
    
    public void setIndexNamePrefix(String indexNamePrefix) {
        this.indexNamePrefix = indexNamePrefix;
    }

    private String getAnyIndexPattern() {
        return indexNamePrefix + "*";
    }
    
    public void dropData() throws IOException {
        for (String indexName : listIndeces()) {
            if (indexName.startsWith(indexNamePrefix)) {
                deleteIndex(indexName);
            }
        }
        typeToIndex.clear();
        ruleToIndex.clear();
    }


    /** The specified list is valid if it,
     *
     * 1. is empty,
     * 2. or contains one or more non-null elements and size is less than
     *    {@link ElasticIndexingStorage#MAX_OBJECT_TYPES_SIZE}.
     *
     * list 1 represents a list that would map to any index pattern (search unconstrained by type)
     * list 2 represents a list that would map to one or more specific index patterns (constrained search)
     *
     *
     * @param objectTypes a list of object types.
     * @throws IOException if list is invalid.
     */
    private void validateObjectTypes(List<String> objectTypes) throws IOException {

        if (objectTypes == null) {
            throw new IllegalArgumentException("Invalid list of object types. List is null.");
        }

        if (objectTypes.isEmpty()) {
            return;
        }

        if (objectTypes.size() > MAX_OBJECT_TYPES_SIZE) {
            throw new IOException("Invalid list of object types. " +
                    "List size exceeds maximum limit of " + MAX_OBJECT_TYPES_SIZE);
        }

        if (objectTypes.contains(null)) {
            throw new IOException("Invalid list of object types. Contains one or more null elements.");
        }
    }


    /** checks that at least one index exists for a type, and throws an exception otherwise.
     *
     * @param objectType
     * @return an index string that matches any version of the type.
     * @throws IOException
     */
    private String checkIndex(final String objectType) throws IOException {
        String ret = typeToIndex.get(objectType);
        if (ret == null) {
            ensureAtLeastOneIndexExists(objectType);
            ret = getAnyTypePattern(objectType);
            typeToIndex.put(objectType, ret);
        }
        return ret;
    }
    
    private void ensureAtLeastOneIndexExists(final String objectType) throws IOException {
        //TODO VERS need to check there aren't duplicate type names based on case
        final String prefix = (indexNamePrefix + objectType + "_").toLowerCase();
        for (final String index: listIndeces()) {
            if (index.startsWith(prefix)) {
                return;
            }
        }
        throw new IOException("No indexes exist for search type " + objectType);
    }

    private String getAnyTypePattern(final String objectType) {
        return (indexNamePrefix + objectType + "_*").toLowerCase();
    }

    /* checks that an index exists for a specific version of a type. If the index
     * does not exist and noCreate is false, creates the index.
     *
     * Returns the elastic search index name.
     */ 
    private String checkIndex(
            final ObjectTypeParsingRules rule,
            final boolean noCreate)
            throws IOException {
        Utils.nonNull(rule, "rule");
        String ret = ruleToIndex.get(rule);
        if (ret == null) {
            ret = toIndexString(rule);
            if (!listIndeces().contains(ret)) {
                if (!noCreate) {
                    System.out.println("Creating Elasticsearch index: " + ret);
                    createTables(ret, rule.getIndexingRules());
                }
            }
            ruleToIndex.put(rule, ret);
        }
        return ret;
    }

    private String toIndexString(final ObjectTypeParsingRules rule) {
        final SearchObjectType objectType = rule.getGlobalObjectType();
        return (indexNamePrefix + objectType.getType() + "_" + objectType.getVersion() +
                (rule.getSubObjectType().isPresent() ? SUBTYPE_INDEX_SUFFIX : ""))
                .toLowerCase();
    }
    
    @Override
    public void indexObject(
            final ObjectTypeParsingRules rule,
            final SourceData data,
            final Instant timestamp,
            final String parentJsonValue,
            final GUID id,
            final ParsedObject obj,
            final boolean isPublic)
            throws IOException, IndexingConflictException {
        final GUID parentID = new GUID(id, null, null);
        indexObjects(rule, data, timestamp, parentJsonValue, parentID,
                ImmutableMap.of(id, obj), isPublic);
    }
    
    // TODO CODE this function should just take a class rather than a zillion arguments
    // the class should ensure consistency of the various fields
    // IO exceptions are thrown for failure on creating or writing to file or contacting ES.
    @Override
    public void indexObjects(
            final ObjectTypeParsingRules rule,
            final SourceData data,
            final Instant timestamp, 
            final String parentJsonValue,
            final GUID pguid,
            final Map<GUID, ParsedObject> idToObj,
            final boolean isPublic)
            throws IOException, IndexingConflictException {
        final Map<GUID, ParsedObject> idToObjCopy = new HashMap<>(idToObj);
        String indexName = checkIndex(rule, false);
        for (GUID id : idToObjCopy.keySet()) {
            GUID parentGuid = new GUID(id.getStorageCode(), id.getAccessGroupId(), 
                    id.getAccessGroupObjectId(), id.getVersion(), null, null);
            if (!parentGuid.equals(pguid)) {
                throw new IllegalStateException("Object GUID doesn't match parent GUID");
            }
        }
        //TODO CODE if there's only a few objects to index, possible speed up by not using tempfile and just making direct API calls
        File tempFile = File.createTempFile("es_bulk_", ".json", tempDir);
        try {
            PrintWriter pw = new PrintWriter(tempFile);
            int lastVersion = loadLastVersion(indexName, pguid, pguid.getVersion());
            final String esParentId = checkParentDoc(indexName, new LinkedHashSet<>(
                    Arrays.asList(pguid)), isPublic, lastVersion).get(pguid);
            if (idToObjCopy.isEmpty()) {
                // there were no search objects parsed from the source object, so just index
                // the general object information
                idToObjCopy.put(pguid, null);
            }
            Map<GUID, String> esIds = lookupDocIds(indexName, idToObjCopy.keySet());
            for (GUID id : idToObjCopy.keySet()) {
                final ParsedObject obj = idToObjCopy.get(id);
                final Map<String, Object> doc = convertObject(id, rule.getGlobalObjectType(), obj,
                        data, timestamp, parentJsonValue, isPublic, lastVersion);
                final Map<String, Object> index = new HashMap<>();
                index.put("_index", indexName);
                index.put("_type", getDataTableName());
                index.put("parent", esParentId);
                if (esIds.containsKey(id)) {
                    index.put("_id", esIds.get(id));
                }
                final Map<String, Object> header = ImmutableMap.of("index", index);
                pw.println(UObject.transformObjectToString(header));
                pw.println(UObject.transformObjectToString(doc));
            }
            pw.close();
            makeRequestBulk("POST", indexName, tempFile);
            updateLastVersionsInData(indexName, pguid, lastVersion);
        } finally {
            tempFile.delete();
        }
        refreshIndex(indexName);
    }
    
    private Map<String, Object> convertObject(
            final GUID id,
            final SearchObjectType objectType,
            final ParsedObject obj, 
            final SourceData data,
            final Instant timestamp,
            final String parentJson,
            final boolean isPublic,
            final int lastVersion) {
        Map<String, List<Object>> indexPart = new LinkedHashMap<>();
        if (obj != null) {
            for (String key : obj.getKeywords().keySet()) {
                indexPart.put(getKeyProperty(key), obj.getKeywords().get(key));
            }
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.putAll(indexPart);
        doc.put(OBJ_GUID, id.toString());
        doc.put(SEARCH_OBJ_TYPE, objectType.getType());
        doc.put(SEARCH_OBJ_TYPE_VER, objectType.getVersion());
        doc.put(SOURCE_TAGS, data.getSourceTags());

        doc.put(OBJ_NAME, data.getName());
        doc.put(OBJ_CREATOR, data.getCreator());
        doc.put(OBJ_COPIER, data.getCopier().orNull());
        doc.put(OBJ_PROV_MODULE, data.getModule().orNull());
        doc.put(OBJ_PROV_METHOD, data.getMethod().orNull());
        doc.put(OBJ_PROV_MODULE_VERSION, data.getVersion().orNull());
        doc.put(OBJ_PROV_COMMIT_HASH, data.getCommitHash().orNull());
        doc.put(OBJ_MD5, data.getMD5().orNull());
        
        doc.put(OBJ_TIMESTAMP, timestamp.toEpochMilli());
        doc.put(OBJ_PREFIX, toGUIDPrefix(id));
        doc.put(OBJ_STORAGE_CODE, id.getStorageCode());
        doc.put(OBJ_ACCESS_GROUP_ID, id.getAccessGroupId());
        doc.put(OBJ_VERSION, id.getVersion());
        doc.put(OBJ_IS_LAST, lastVersion == id.getVersion());
        doc.put(OBJ_PUBLIC, isPublic);
        doc.put(OBJ_SHARED, false);
        if (obj != null) {
            doc.put("ojson", obj.getJson());
            doc.put("pjson", parentJson);
        }
        return doc;
    }

    @Override
    public void flushIndexing(final ObjectTypeParsingRules rule) throws IOException {
        refreshIndex(checkIndex(rule, true));
    }
    
    private Map<GUID, String> lookupDocIds(String indexName, Set<GUID> guids) throws IOException {

        // doc = {"query": {"bool": {"filter": [{"terms": {"guid": [guids]}}]}}}
        Map<String, Object> doc = ImmutableMap.of(
                "query", ImmutableMap.of(
                        "bool", ImmutableMap.of(
                                "filter", Arrays.asList(ImmutableMap.of(
                                        "terms", ImmutableMap.of(
                                                "guid", guids.stream().map(u -> u.toString())
                                                        .collect(Collectors.toList())))))));

        String urlPath = "/" + indexName + "/" + getDataTableName() + "/_search";
        Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<GUID, String> ret = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            String id = (String)hit.get("_id");
            @SuppressWarnings("unchecked")
            Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            GUID guid = new GUID((String) obj.get("guid"));
            ret.put(guid, id);
        }
        return ImmutableMap.copyOf(ret);
    }

    private Map<GUID, String> lookupParentDocIds(String indexName, Set<GUID> guids) throws IOException {
        // doc = {"query": {"bool": {"filter": [{"terms": {"pguid": [guids]}}]}}}
        Map<String, Object> doc =
                ImmutableMap.of("query",
                  ImmutableMap.of("bool",
                    ImmutableMap.of("filter",
                      Arrays.asList(ImmutableMap.of("terms",
                                      ImmutableMap.of("pguid",
                        guids.stream().map(u -> u.toString()).collect(Collectors.toList())))))));

        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_search";
        Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<GUID, String> ret = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            String id = (String)hit.get("_id");
            @SuppressWarnings("unchecked")
            Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            GUID guid = new GUID((String)obj.get("pguid"));
            ret.put(guid, id);
        }
        return ImmutableMap.copyOf(ret);
    }

    public Map<String, Set<GUID>> groupParentIdsByIndex(Set<GUID> ids) throws IOException {
        Set<String> parentIds = new LinkedHashSet<>();
        for (GUID guid : ids) {
            parentIds.add(new GUID(guid.getStorageCode(), guid.getAccessGroupId(), 
                    guid.getAccessGroupObjectId(), guid.getVersion(), null, null).toString());
        }

        // doc = {"query": {"bool": {"filter": {"terms: ": {"pguid": [ids]}}}},
        //        "_source": ["pguid"]}
        Map<String, Object> doc =
                ImmutableMap.of("query",
                   ImmutableMap.of("bool",
                      ImmutableMap.of("filter",
                         ImmutableMap.of("terms",
                            ImmutableMap.of("pguid", parentIds)))),
                                            "_source", Arrays.asList("pguid"));

        String urlPath = "/" + indexNamePrefix + "*/" + getAccessTableName() + "/_search";
        Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<String, Set<GUID>> ret = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            String indexName = (String)hit.get("_index");
            Set<GUID> retSet = ret.get(indexName);
            if (retSet == null) {
                retSet = new LinkedHashSet<>();
                ret.put(indexName, retSet);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            GUID guid = new GUID((String)obj.get("pguid"));
            retSet.add(guid);
        }
        return ImmutableMap.copyOf(ret);
    }

    private Map<String, Object> createFilter(String queryType, String keyName, Object value) {
        Map<String, Object> term = new LinkedHashMap<>();
        // term = {keyname: value}?
        if (keyName != null) {
            term.put(keyName, value);
        }

        Map<String, Object> termWrapper = new LinkedHashMap<>();
        termWrapper.put(queryType, ImmutableMap.copyOf(term));
        // return = {queryType: {keyname: value}?}
        return ImmutableMap.copyOf(termWrapper);
    }

    private Map<String, Object> createRangeFilter(String keyName, Object gte, Object lte) {
        Map<String, Object> range = new LinkedHashMap<>();
        if (gte != null) {
            range.put("gte", gte);
        }
        if (lte != null) {
            range.put("lte", lte);
        }

        Map<String, Object> termWrapper = ImmutableMap.of("range",
                                             ImmutableMap.of(keyName, ImmutableMap.copyOf(range)));

        return termWrapper;
    }
    
    // throws IOexceptions for elastic connection issues & deserializaion issues
    @Override
    public Map<GUID, Boolean> checkParentGuidsExist(final Set<GUID> guids) throws IOException {
        Set<GUID> parentGUIDs = guids.stream().map(guid -> new GUID(
                guid.getStorageCode(),
                guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(),
                guid.getVersion(),
                null, null))
                .collect(Collectors.toSet());
        final String indexName = getAnyIndexPattern();
        // In next operation map value may contain one of possible parents in case objectType==null
        final Map<GUID, String> map = lookupParentDocIds(indexName, parentGUIDs);
        return ImmutableMap.copyOf(parentGUIDs.stream().collect(
                Collectors.toMap(Function.identity(), guid -> map.containsKey(guid))));
    }

    private Integer loadLastVersion(String reqIndexName, GUID parentGUID, 
            Integer processedVersion) throws IOException {
        if (reqIndexName == null) {
            reqIndexName = getAnyIndexPattern();
        }
        String prefix = toGUIDPrefix(parentGUID);

        // doc = {"query": {"bool": {"filter": [{"term": {"prefix": prefix}}]}}}
        Map<String, Object> doc = ImmutableMap.of("query",
                                     ImmutableMap.of("bool",
                                        ImmutableMap.of("filter",
                                           Arrays.asList(ImmutableMap.of(
                                              "term",
                                                  ImmutableMap.of("prefix", prefix))))));

        String urlPath = "/" + reqIndexName + "/" + getAccessTableName() + "/_search";
        Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        Integer ret = null;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            @SuppressWarnings("unchecked")
            Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            int version = (Integer)obj.get("version");
            if (ret == null || ret < version) {
                ret = version;
            }
        }
        if (processedVersion != null && (ret == null || ret < processedVersion)) {
            ret = processedVersion;
        }
        return ret;
    }
    
    private int updateLastVersionsInData(String indexName, GUID parentGUID,
            int lastVersion) throws IOException, IndexingConflictException {
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }

        // query = {"bool": {"filter": [{"term": {"prefix": prefix}}]}}
        Map<String, Object> query = ImmutableMap.of("bool",
                                       ImmutableMap.of("filter",
                                               Arrays.asList(ImmutableMap.of("term",
                                                       ImmutableMap.of("prefix",
                                                               toGUIDPrefix(parentGUID))))));

        // params = {"lastver": lastVersion}
        final Map<String, Object> params = ImmutableMap.of("lastver", lastVersion);

        // script = {"inline": "ctx._source.islast = (ctx._source.version == params.lastver)",
        //           "params": {"lastver": lastVersion}}
        Map<String, Object> script = ImmutableMap.of(
                "inline", "ctx._source.islast = (ctx._source.version == params.lastver);",
                "params", params);

        // doc = {"query": {"bool": {"filter": [{"term": {"prefix": prefix}}]}},
        //        "script": {"inline": "ctx._source.islast = (ctx._source.version == params.lastver)",
        //                   "params": {"lastver": lastVersion}}}
        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getDataTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated");
    }

    private Map<GUID, String> checkParentDoc(String indexName, Set<GUID> parentGUIDs, 
            boolean isPublic, int lastVersion) throws IOException, IndexingConflictException {
        boolean changed = false;
        Map<GUID, String> ret = new LinkedHashMap<>(lookupParentDocIds(indexName, parentGUIDs));
        for (GUID parentGUID : parentGUIDs) {
            if (ret.containsKey(parentGUID)) {
                continue;
            }
            String prefix = toGUIDPrefix(parentGUID);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("pguid", parentGUID.toString());
            doc.put("prefix", prefix);
            doc.put("version", parentGUID.getVersion());
            Set<Integer> accessGroupIds = new LinkedHashSet<>(Arrays.asList(
                    ADMIN_ACCESS_GROUP));
            if (parentGUID.getAccessGroupId() != null) {
                accessGroupIds.add(parentGUID.getAccessGroupId());
            }
            if (isPublic) {
                accessGroupIds.add(PUBLIC_ACCESS_GROUP);
            }
            Set<Integer> lastinGroupIds = parentGUID.getVersion() == lastVersion ? 
                    accessGroupIds : Collections.emptySet();
            doc.put("lastin", lastinGroupIds);
            doc.put("groups", accessGroupIds);
            doc.put("extpub", new ArrayList<Integer>());
            Response resp = makeRequest("POST", "/" + indexName + "/" + getAccessTableName() + "/",
                    doc);
            @SuppressWarnings("unchecked")
            Map<String, Object> data = UObject.getMapper().readValue(
                    resp.getEntity().getContent(), Map.class);
            ret.put(parentGUID, (String)data.get("_id"));
            changed = true;
            updateAccessGroupForVersions(indexName, parentGUID, lastVersion,
                    parentGUID.getAccessGroupId(), isPublic, true);
        }
        if (changed) {
            refreshIndex(indexName);
        }
        return ImmutableMap.copyOf(ret);
    }
    
    private static final String UPDATE_ACC_GRP_VERS_TEMPLATE =
            "if (ctx._source.lastin.indexOf(params.%1$s) >= 0) {\n" +
            "  if (ctx._source.version != params.lastver) {\n" +
            "    ctx._source.lastin.remove(ctx._source.lastin.indexOf(params.%1$s));\n" +
            "    if (ctx._source.extpub.indexOf(params.%1$s) >= 0) {\n" + 
            "      ctx._source.extpub.remove(ctx._source.extpub.indexOf(params.%1$s));\n" +
            "    }\n" +
            "  }\n" +
            "} else {\n" +
            "  if (ctx._source.version == params.lastver) {\n" +
            "    ctx._source.lastin.add(params.%1$s);\n" +
            "    if (ctx._source.groups.indexOf(params.%1$s) < 0) {\n" +
            "      ctx._source.groups.add(params.%1$s);\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    /* calling this method with accessGroupId == null and both booleans false is an error. */
    private boolean updateAccessGroupForVersions(
            String indexName,
            final GUID guid,
            final int lastVersion,
            final Integer accessGroupId,
            final boolean includePublicAccessID,
            final boolean includeAdminAccessID)
            throws IOException, IndexingConflictException {
        /* this method will cause at most 6 script compilations, which seems like a lot...
         * Could make the script always the same and put in ifs but this should be ok for now.
         */
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }

        // query = {"bool": {"must": [{"term": {"prefix": prefix}?}]}}
        Map<String, Object> query = ImmutableMap.of("bool",
                ImmutableMap.of("must",
                        Arrays.asList(
                                createFilter("term", "prefix", toGUIDPrefix(guid)))));

        // params = {"lastver": lastVersion,
        //           ("accgrp": accessGroupId)?,
        //           ("pubaccgrp": -1)?,
        //           ("pubaccgrp": -2)?}
        StringBuilder inline = new StringBuilder();
        final Map<String, Object> params = new HashMap<>();
        params.put("lastver", lastVersion);
        if (accessGroupId != null) {
            inline.append(String.format(UPDATE_ACC_GRP_VERS_TEMPLATE, "accgrp"));
            params.put("accgrp", accessGroupId);
        }
        if (includePublicAccessID) {
            inline.append(String.format(UPDATE_ACC_GRP_VERS_TEMPLATE, "pubaccgrp"));
            params.put("pubaccgrp", PUBLIC_ACCESS_GROUP);
        }
        if (includeAdminAccessID) {
            inline.append(String.format(UPDATE_ACC_GRP_VERS_TEMPLATE, "adminaccgrp"));
            params.put("adminaccgrp", ADMIN_ACCESS_GROUP);
        }
        Map<String, Object> script = new LinkedHashMap<>();
        script.put("inline", inline.toString());
        script.put("params", ImmutableMap.copyOf(params));

        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    private boolean removeAccessGroupForVersion(String indexName, GUID guid, 
            int accessGroupId) throws IOException, IndexingConflictException {
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }
        // This flag shows that we work with other than physical access group this object exists in.
        boolean fromAllGroups = accessGroupId != guid.getAccessGroupId();
        String pguid = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), guid.getVersion(), null, null).toString();


        Map<String, Object> query = ImmutableMap.of("bool",
                                       ImmutableMap.of("must",
                                          Arrays.asList(
                                                  createFilter("term", "pguid", pguid),
                                                  createFilter("term", "lastin", accessGroupId))));

        final Map<String, Object> params = ImmutableMap.of("accgrp", accessGroupId);
        Map<String, Object> script = ImmutableMap.of(
                "inline",
                "ctx._source.lastin.remove(ctx._source.lastin.indexOf(params.accgrp));\n" +
                "if (ctx._source.extpub.indexOf(params.accgrp) >= 0) {\n" +
                "  ctx._source.extpub.remove(ctx._source.extpub.indexOf(params.accgrp));\n" +
                "}\n" +
                (fromAllGroups ? (
                        "int pos = ctx._source.groups.indexOf(params.accgrp);\n" +
                                "if (pos >= 0) {\n" +
                                "  ctx._source.groups.remove(pos);\n" +
                                "}\n") : ""),
                "params", params);

        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    private boolean updateBooleanFieldInData(String indexName, GUID parentGUID,
            String field, boolean value) throws IOException, IndexingConflictException {
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }

        Map<String, Object> query = ImmutableMap.of("bool",
                                       ImmutableMap.of("must",
                Arrays.asList(createFilter("term", "prefix", toGUIDPrefix(parentGUID)),
                              createFilter("term", "version", parentGUID.getVersion()))));

        final Map<String, Object> params = ImmutableMap.of("field", field,
                                                           "value", value);
        Map<String, Object> script = ImmutableMap.of("inline",
                                                     "ctx._source[params.field] = params.value;",
                                                     "params", params);

        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getDataTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    private String toGUIDPrefix(GUID parentGUID) {
        return new GUID(parentGUID.getStorageCode(), parentGUID.getAccessGroupId(),
                parentGUID.getAccessGroupObjectId(), null, null, null).toString();
    }
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public int setNameOnAllObjectVersions(final GUID object, final String newName)
            throws IOException, IndexingConflictException {
        return setFieldOnObject(object, OBJ_NAME, newName, true);
    }
    
    /* expects that GUID does not have sub object info */
    // TODO CODE allow providing index name for optimization
    private int setFieldOnObject(
            final GUID object,
            final String field,
            final Object value,
            final boolean allVersions)
            throws IOException, IndexingConflictException {
        final String index = getAnyIndexPattern();
        final Map<String, Object> query;
        if (allVersions) {
            query = createFilter("term", "prefix", toGUIDPrefix(object));
        } else {
            query = createFilter("term", "guid", object.toString());
        }
        final Map<String, Object> script = ImmutableMap.of(
                "inline", "ctx._source[params.field] = params.value",
                "params", ImmutableMap.of("field", field, "value", value));
        final Map<String, Object> doc = ImmutableMap.of(
                "query", query,
                "script", script);
        final String urlPath = "/" + index + "/" + getDataTableName() + "/_update_by_query";
        final Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        final Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (int) data.get("updated");
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void shareObjects(Set<GUID> guids, int accessGroupId, 
            boolean isExternalPublicGroup) throws IOException, IndexingConflictException {
        Map<String, Set<GUID>> indexToGuids = groupParentIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            Set<GUID> toAddExtPub = new LinkedHashSet<GUID>();
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (updateAccessGroupForVersions(indexName, guid, guid.getVersion(), accessGroupId,
                        false, false)) {
                    needRefresh = true;
                }
                if (accessGroupId == PUBLIC_ACCESS_GROUP) {
                    if (updateBooleanFieldInData(indexName, guid, "public", true)) {
                        needRefresh = true;
                    }
                } else if (accessGroupId != guid.getAccessGroupId()) {
                    if (updateBooleanFieldInData(indexName, guid, "shared", true)) {
                        needRefresh = true;
                    }
                    if (isExternalPublicGroup) {
                        toAddExtPub.add(guid);
                    }
                }
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
            if (!toAddExtPub.isEmpty()) {
                needRefresh = false;
                for (GUID guid : indexToGuids.get(indexName)) {
                    if (addExtPubForVersion(indexName, guid, accessGroupId)) {
                        needRefresh = true;
                    }
                }
                if (needRefresh) {
                    refreshIndex(indexName);
                }
            }
        }
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void unshareObjects(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException {
        Map<String, Set<GUID>> indexToGuids = groupParentIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (removeAccessGroupForVersion(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
                if (accessGroupId == PUBLIC_ACCESS_GROUP) {
                    if (updateBooleanFieldInData(indexName, guid, "public", false)) {
                        needRefresh = true;
                    }
                }
                //TODO NOW how is share bit unset?
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
        }
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void deleteAllVersions(final GUID guid) throws IOException, IndexingConflictException {
        // could optimize later by making LLV return the index name
        final Integer ver = loadLastVersion(null, guid, null);
        if (ver == null) {
            //TODO NOW throw exception? means a delete event occurred when there were no objects
            return;
        }
        final String indexName = getAnyIndexPattern();
        setFieldOnObject(withVersion(guid, ver), "islast", false, false);
        // -3 is a hack to always remove access groups
        updateAccessGroupForVersions(indexName, guid, -3, guid.getAccessGroupId(), false, false);
        /* changing the public field doesn't make a ton of sense - the object is still in a public
         * workspace. 
         * TODO NOW add a deleted flag, use that instead.
         */
//        setFieldOnObjectForAllVersions(guid, "public", false);
        //TODO NOW this doesn't handle removing public (-1) from the access doc because it can't know that's the right thing to do
        //TODO NOW admin access group id has same problem as public access group id
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void undeleteAllVersions(final GUID guid)
            throws IOException, IndexingConflictException {
        // could optimize later by making LLV return the index name
        final Integer ver = loadLastVersion(null, guid, null);
        if (ver == null) {
            //TODO NOW throw exception? means an undelete event occurred when there were no objects
            return;
        }
        updateLastVersionsInData(null, guid, ver);
        updateAccessGroupForVersions(null, guid, ver, guid.getAccessGroupId(), false, true);
        // TODO NOW remove deleted flag from delete all versions
        
    }
    
    private GUID withVersion(final GUID guid, int ver) {
        return new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), ver, null, null);
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void publishObjects(Set<GUID> guids) throws IOException, IndexingConflictException {
        shareObjects(guids, PUBLIC_ACCESS_GROUP, false);
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void unpublishObjects(Set<GUID> guids) throws IOException, IndexingConflictException {
        unshareObjects(guids, PUBLIC_ACCESS_GROUP);
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void publishAllVersions(final GUID guid) throws IOException, IndexingConflictException {
        setFieldOnObject(guid, "public", true, true);
    }
    
    //IO exception thrown for deserialization & elasticsearch contact errors
    @Override
    public void unpublishAllVersions(final GUID guid)
            throws IOException, IndexingConflictException {
        setFieldOnObject(guid, "public", false, true);
    }

    private boolean addExtPubForVersion(String indexName, GUID guid, 
            int accessGroupId) throws IOException, IndexingConflictException {
        // Check that we work with other than physical access group this object exists in.
        if (accessGroupId == guid.getAccessGroupId()) {
            throw new IllegalStateException("Access group should be external");
        }
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }
        String pguid = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), guid.getVersion(), null, null).toString();

        Map<String, Object> query = ImmutableMap.of("bool",
                                       ImmutableMap.of("must",
                          Arrays.asList(createFilter("term", "pguid", pguid))));

        final Map<String, Object> params = ImmutableMap.of("accgrp", accessGroupId);
        Map<String, Object> script = ImmutableMap.of(
                "inline",
                "if (ctx._source.extpub.indexOf(params.accgrp) < 0) {\n" +
                "  ctx._source.extpub.add(params.accgrp);\n" +
                "}\n",
                "params", params);

        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    @Override
    public void publishObjectsExternally(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException {
        Map<String, Set<GUID>> indexToGuids = groupParentIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (addExtPubForVersion(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
        }
    }

    private boolean removeExtPubForVersion(String indexName, GUID guid, 
            int accessGroupId) throws IOException, IndexingConflictException {
        // Check that we work with other than physical access group this object exists in.
        if (accessGroupId == guid.getAccessGroupId()) {
            throw new IllegalStateException("Access group should be external");
        }
        if (indexName == null) {
            indexName = getAnyIndexPattern();
        }
        String pguid = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), guid.getVersion(), null, null).toString();

        Map<String, Object> query = ImmutableMap.of("bool",
                                       ImmutableMap.of("must",
                             Arrays.asList(createFilter("term", "pguid", pguid),
                                           createFilter("term", "extpub", accessGroupId))));

        final Map<String, Object> params = ImmutableMap.of("accgrp", accessGroupId);
        Map<String, Object> script = ImmutableMap.of(
                "inline",
                "ctx._source.extpub.remove(ctx._source.extpub.indexOf(params.accgrp));\n",
                "params", params);

        Map<String, Object> doc = ImmutableMap.of("query", query,
                                                  "script", script);

        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    @Override
    public void unpublishObjectsExternally(Set<GUID> guids, int accessGroupId)
            throws IOException, IndexingConflictException {
        Map<String, Set<GUID>> indexToGuids = groupParentIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (removeExtPubForVersion(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
        }
    }
    
    @Override
    public List<ObjectData> getObjectsByIds(Set<GUID> ids) throws IOException {
        PostProcessing pp = new PostProcessing();
        pp.objectInfo = true;
        pp.objectData = true;
        pp.objectKeys = true;
        return getObjectsByIds(ids, pp);
    }

    private Map<String, Object> createHighlightQuery(){
        return  ImmutableMap.of("fields",
                    ImmutableMap.of("*",
                            ImmutableMap.of("require_field_match", false)));
    }

    @Override
    public List<ObjectData> getObjectsByIds(final Set<GUID> ids, final PostProcessing pp)
            throws IOException {

        final Map<String, Object> query = ImmutableMap.of("bool",
                                        ImmutableMap.of("filter",
                                           ImmutableMap.of("terms",
                ImmutableMap.of("guid", ids.stream().map(u -> u.toString()).collect(Collectors.toList())))));

        final Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("query", query);

        if (Objects.nonNull(pp) && pp.objectHighlight) {
            doc.put("highlight", createHighlightQuery());
        }

        final String urlPath = "/" + indexNamePrefix + "*/" + getDataTableName() + "/_search";
        final Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        final Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        final List<ObjectData> ret = new ArrayList<>();
        @SuppressWarnings("unchecked")
        final Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            @SuppressWarnings("unchecked")
            final Map<String, List<String>> highlightRes =
                    (Map<String, List<String>>) hit.get("highlight");
            final ObjectData item = buildObjectData(obj, highlightRes, pp);
            ret.add(item);
        }
        return ret;
    }

    private ObjectData buildObjectData(
            final Map<String, Object> obj,
            final Map<String, List<String>> highlight,
            final PostProcessing pp) {
        // TODO: support sub-data selection based on objectDataIncludes (acts on parent json or sub object json)

        GUID guid = new GUID((String) obj.get("guid"));
        final ObjectData.Builder b = ObjectData.getBuilder(guid);
        if (pp.objectInfo) {
            b.withNullableObjectName((String) obj.get(OBJ_NAME));
            b.withNullableCreator((String) obj.get(OBJ_CREATOR));
            b.withNullableCopier((String) obj.get(OBJ_COPIER));
            b.withNullableModule((String) obj.get(OBJ_PROV_MODULE));
            b.withNullableMethod((String) obj.get(OBJ_PROV_METHOD));
            b.withNullableModuleVersion((String) obj.get(OBJ_PROV_MODULE_VERSION));
            b.withNullableCommitHash((String) obj.get(OBJ_PROV_COMMIT_HASH));
            b.withNullableMD5((String) obj.get(OBJ_MD5));
            b.withNullableType(new SearchObjectType(
                    (String) obj.get(SEARCH_OBJ_TYPE),
                    (Integer) obj.get(SEARCH_OBJ_TYPE_VER)));
            // sometimes this is a long, sometimes it's an int
            b.withNullableTimestamp(Instant.ofEpochMilli(
                    ((Number) obj.get(OBJ_TIMESTAMP)).longValue()));
            @SuppressWarnings("unchecked")
            final List<String> sourceTags = (List<String>) obj.get(SOURCE_TAGS);
            if (sourceTags != null) {
                for (final String tag: sourceTags) {
                    b.withSourceTag(tag);
                }
            }
        }
        if (pp.objectData) {
            final String ojson = (String) obj.get("ojson");
            if (ojson != null) {
                b.withNullableData(UObject.transformStringToObject(
                        ojson, Object.class));
            }
            final String pjson = (String) obj.get("pjson");
            if (pjson != null) {
                b.withNullableParentData(UObject.transformStringToObject(pjson, Object.class));
            }
        }
        if (pp.objectKeys) {
            for (final String key : obj.keySet()) {
                if (key.startsWith("key.")) {
                    final Object objValue = obj.get(key);
                    String textValue;
                    if (objValue instanceof List) {
                        @SuppressWarnings("unchecked")
                        final List<Object> objValue2 = (List<Object>) objValue;
                        textValue = objValue2.stream().map(Object::toString)
                                .collect(Collectors.joining(", "));
                    } else {
                        textValue = String.valueOf(objValue);
                    }
                    b.withKeyProperty(stripKeyPrefix(key), textValue);
                }
            }
        }

        //because elastic sometimes returns highlight as null instead of empty map.
        if (pp.objectHighlight && highlight != null) {
            for(final String key : highlight.keySet()) {
                b.withHighlight(getReadableKeyNames(key, guid), highlight.get(key));
            }    
        }

        return b.build();
    }

    private String getReadableKeyNames(final String key, final GUID guid)
            throws IllegalStateException{
        if (key.startsWith("key.")) {
            return stripKeyPrefix(key);
        } else if(READABLE_NAMES.containsKey(key)) {
            return READABLE_NAMES.get(key);
        } else {
            //this should not happen. Untested
            String message = "Object with guid " + guid.toString() + " has unexpected key: " + key;
            throw new IllegalStateException(message);
        }
    }
    private String stripKeyPrefix(final String key){
        return key.substring(4);
    }

    private Map<String, Object> createPublicShouldBlock(boolean withAllHistory) {
        List<Object> must0List = new ArrayList<>();
        must0List.add(createFilter("term", "public", true));
        if (!withAllHistory) {
            must0List.add(createFilter("term", "islast", true));
        }

        Map<String, Object> bool0Wrapper = ImmutableMap.of("bool",
                                              ImmutableMap.of("must", must0List));
        return bool0Wrapper;
    }
    
    private Map<String, Object> createOwnerShouldBlock(AccessFilter accessFilter) {
        List<Object> must1List = new ArrayList<>();
        if (!accessFilter.isAdmin) {
            Set<Integer> accGroups = accessFilter.accessGroupIds;
            if (accGroups == null) {
                accGroups = Collections.emptySet();
            }
            must1List.add(createFilter("terms", "accgrp", accGroups));
        }
        if (!accessFilter.withAllHistory) {
            must1List.add(createFilter("term", "islast", true));
        }

        Map<String, Object> bool1Wrapper = ImmutableMap.of("bool",
                                              ImmutableMap.of("must", must1List));

        return bool1Wrapper;
    }
    
    private Map<String, Object> createSharedShouldBlock(Map<String, Object> mustForShared) {
        List<Object> must2List = new ArrayList<>(Arrays.asList(
                createFilter("term", "shared", true), mustForShared));

        Map<String, Object> bool2Wrapper = ImmutableMap.of("bool",
                                              ImmutableMap.of("must", must2List));
        return bool2Wrapper;
    }
    
    //TODO VERS should this return SearchObjectType -> Integer map? Maybe an option to combine versions
    @Override
    public Map<String, Integer> searchTypes(
            final MatchFilter matchFilter,
            final AccessFilter accessFilter)
            throws IOException {
        Map<String, Object> mustForShared = createAccessMustBlock(accessFilter);
        if (mustForShared == null) {
            return Collections.emptyMap();
        }
        //TODO VERS if this aggregates by type version, need to add the version field to the terms
        Map<String, Object> aggs = ImmutableMap.of("types",
                                      ImmutableMap.of("terms",
                                         ImmutableMap.of("field", SEARCH_OBJ_TYPE)));

        Map<String, Object> doc = ImmutableMap.of(
                "query", createObjectQuery(matchFilter, accessFilter),
                "aggregations", aggs,
                "size", 0);

        String urlPath = "/" + indexNamePrefix + "*" +
                (matchFilter.isExcludeSubObjects() ? EXCLUDE_SUB_OJBS_URL_SUFFIX : "") +
                "/" + getDataTableName() + "/_search";
        Response resp = makeRequestNoConflict("GET", urlPath, doc);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> aggMap = (Map<String, Object>) data.get("aggregations");
        @SuppressWarnings("unchecked")
        Map<String, Object> typeMap = (Map<String, Object>) aggMap.get("types");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) typeMap.get("buckets");
        Map<String, Integer> ret = new TreeMap<>();
        for (Map<String, Object> bucket : buckets) {
            String objType = (String)bucket.get("key");
            Integer count = (Integer)bucket.get("doc_count");
            ret.put(objType, count);
        }
        return ImmutableMap.copyOf(ret);
    }

    private Map<String, Object> createObjectQuery(
            final MatchFilter matchFilter,
            final AccessFilter accessFilter) {
        
        final List<Object> shouldList = new ArrayList<>();
        // Public block (we exclude it for admin because it's covered by owner block)
        if (accessFilter.withPublic && !accessFilter.isAdmin) {
            shouldList.add(createPublicShouldBlock(accessFilter.withAllHistory));
        }

        // Owner block
        shouldList.add(createOwnerShouldBlock(accessFilter));

        // Shared block
        shouldList.add(createSharedShouldBlock(createAccessMustBlock(accessFilter)));
        // Rest of query
        
        final Map<String, Object> bool = new HashMap<>();
        bool.putAll(prepareMatchFilters(matchFilter));
        bool.put("filter", Arrays.asList(ImmutableMap.of("bool", ImmutableMap.of(
                "should", shouldList))));
        return  ImmutableMap.of("bool", bool);
    }
    
    @Override
    public FoundHits searchIds(
            final List<String> objectTypes,
            final MatchFilter matchFilter,
            final List<SortingRule> sorting,
            final AccessFilter accessFilter,
            final Pagination pagination)
            throws IOException {
        return queryHits(objectTypes, matchFilter, sorting, accessFilter, pagination, null);
    }

    @Override
    public FoundHits searchObjects(
            final List<String> objectTypes,
            final MatchFilter matchFilter,
            final List<SortingRule> sorting,
            final AccessFilter accessFilter,
            final Pagination pagination,
            final PostProcessing postProcessing)
            throws IOException {
        return queryHits(objectTypes, matchFilter, sorting, accessFilter, pagination,
                postProcessing);
    }
    
 // this is only used for tests
    public Set<GUID> searchIds(
            final List<String> objectTypes,
            final MatchFilter matchFilter, 
            final List<SortingRule> sorting,
            final AccessFilter accessFilter)
            throws IOException {
        return searchIds(objectTypes, matchFilter, sorting, accessFilter, null).guids;
    }

    private Map<String, Object> prepareMatchFilters(MatchFilter matchFilter) {
        final List<Map<String, Object>> matches = new ArrayList<>();
        if (matchFilter.getFullTextInAll().isPresent()) {
            final LinkedHashMap<String, Object> query = new LinkedHashMap<>();
            query.put("query", matchFilter.getFullTextInAll().get());
            query.put("operator", "and");

            final LinkedHashMap<String, Object> allQuery = new LinkedHashMap<>();
            allQuery.put("_all", query);

            final LinkedHashMap<String, Object> match = new LinkedHashMap<>();
            match.put("match",allQuery);
            matches.add(match);
        }
        // TODO: support for matchFilter.accessGroupId (e.g. reduce search scope to one group)
        /*if (matchFilter.accessGroupId != null) {
            ret.add(createAccessMustBlock(new LinkedHashSet<>(Arrays.asList(
                    matchFilter.accessGroupId)), withAllHistory));
        }*/
        if (matchFilter.getObjectName().isPresent()) {
                                                    // this seems like a bug...?
            matches.add(createFilter("match", OBJ_NAME, matchFilter.getFullTextInAll().get()));
        }
        for (final String keyName : matchFilter.getLookupInKeys().keySet()) {
            final MatchValue value = matchFilter.getLookupInKeys().get(keyName);
            final String keyProp = getKeyProperty(keyName);
            if (value.value != null) {
                matches.add(createFilter("term", keyProp, value.value));
            } else if (value.minInt != null || value.maxInt != null) {
                matches.add(createRangeFilter(keyProp, value.minInt, value.maxInt));
            } else if (value.minDate != null || value.maxDate != null) {
                matches.add(createRangeFilter(keyProp, value.minDate, value.maxDate));
            } else if (value.minDouble != null || value.maxDouble != null) {
                matches.add(createRangeFilter(keyProp, value.minDouble, value.maxDouble));
            }
        }
        if (matchFilter.getTimestamp().isPresent()) {
            matches.add(createRangeFilter(OBJ_TIMESTAMP, matchFilter.getTimestamp().get().minDate, 
                    matchFilter.getTimestamp().get().maxDate));
        }
        
        
        final Map<String, Object> ret = new HashMap<>();
        if (!matchFilter.getSourceTags().isEmpty()) {
            final Map<String, Object> tagsQuery = ImmutableMap.of("terms", ImmutableMap.of(
                    SOURCE_TAGS, matchFilter.getSourceTags()));
            if (matchFilter.isSourceTagsBlacklist()) {
                ret.put("must_not", tagsQuery);
            } else {
                matches.add(tagsQuery);
            }
        }
        // TODO: support parent guid (reduce search scope to one object, e.g. features of one geneom)
        if (ret.isEmpty() && matches.isEmpty()) {
            matches.add(createFilter("match_all", null, null));
        }
        if (!matches.isEmpty()) {
            ret.put("must", matches);
        }
        return ret;
    }
    
    private Map<String, Object> createAccessMustBlock(AccessFilter accessFilter) {
        Set<Integer> accessGroupIds = new LinkedHashSet<>();
        if (accessFilter.isAdmin) {
            accessGroupIds.add(ADMIN_ACCESS_GROUP);
        } else {
            if (accessFilter.accessGroupIds != null) {
                accessGroupIds.addAll(accessFilter.accessGroupIds);
            }
            if (accessFilter.withPublic) {
                accessGroupIds.add(PUBLIC_ACCESS_GROUP);
            }
        }
        if (accessGroupIds.isEmpty()) {
            return null;
        }
        return createAccessMustBlock(accessGroupIds, accessFilter.withAllHistory,
                accessFilter.withPublic);
    }
    
    private Map<String, Object> createAccessMustBlock(Set<Integer> accessGroupIds, 
            boolean withAllHistory, boolean withPublic) {
        // should = []
        List<Object> should = new ArrayList<>();

        // match = {groupListProp: [accessGroupIds]}
        String groupListProp = withAllHistory ? "groups" : "lastin";  // I think lastin means last version even though version is orthogonal to the concept of groups?

        // terms = {"terms": { groupListProp: [accessGroupIds]}}
        Map<String, Object> terms = ImmutableMap.of("terms",
                                       ImmutableMap.of(groupListProp, accessGroupIds));


        // should = [{"terms": {groupListProp: [accessGroupIds]}}]
        should.add(terms);

        if (withPublic) {
            // Case of public workspaces containing DataPalette referencing to given object
            // We basically check how many public workspaces (external comparing to home 
            // workspace of given object) have DataPalettes referencing given object (and 
            // version). If this number is 0 then object+version is not visible as public 
            // through DataPalettes. If it's >0 (which is the same as existence of keywords 
            // in 'extpub') then it's visible.
            // exists = {"field", "extpub"}

            // existsWrapper = {"exists": {"field", "extpub"}}
            Map<String, Object> existwrapper = ImmutableMap.of("exists",
                                                  ImmutableMap.of("field", "extpub"));

            // should = [{"terms": {groupListProp: [accessGroupIds]}}
            //           {"exists": {"field", "extpub"}}]
            should.add(existwrapper);
        }

        // hasParentWrapper = {"hasParent": {"parent_type": "access",
        //                                   "query": {"bool": {"should": [{"terms": {groupListProp: [accessGroupIds]}}
        //                                                     {"exists": {"field", "extpub"}}?]}}}}
        Map<String, Object> hasParentWrapper = ImmutableMap.of("has_parent",
                                                  ImmutableMap.of("parent_type", getAccessTableName(),
                                                                  "query", ImmutableMap.of("bool",
                                                                              ImmutableMap.of("should", should))));
        return hasParentWrapper;
    }
    
    private FoundHits queryHits(
            final List<String> objectTypes,
            final MatchFilter matchFilter, 
            List<SortingRule> sorting,
            final AccessFilter accessFilter,
            final Pagination pg,
            final PostProcessing pp)
            throws IOException {
        // initialize args
        int pgStart = pg == null || pg.start == null ? 0 : pg.start;
        int pgCount = pg == null || pg.count == null ? 50 : pg.count;
        Pagination pagination = new Pagination(pgStart, pgCount);
        if (sorting == null || sorting.isEmpty()) {
            final SortingRule sr = SortingRule.getStandardPropertyBuilder(R_OBJ_TIMESTAMP).build();
            sorting = Arrays.asList(sr);
        }
        FoundHits ret = new FoundHits();
        ret.pagination = pagination;
        ret.sortingRules = sorting;

        final Map<String, Object> mustForShared = createAccessMustBlock(accessFilter);
        if (mustForShared == null) {
            ret.total = 0;
            ret.guids = Collections.emptySet();
            return ret;
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        
        doc.put("query", createObjectQuery(matchFilter, accessFilter));
        if (Objects.nonNull(pp) && pp.objectHighlight) {
            doc.put("highlight", createHighlightQuery());
        }
        doc.put("from", pagination.start);
        doc.put("size", pagination.count);

        boolean loadObjects = pp != null &&
                (pp.objectInfo || pp.objectData || pp.objectKeys || pp.objectHighlight);
        if (!loadObjects) {
            doc.put("_source", Arrays.asList("guid"));
        }
        doc.put("sort", createSortQuery(sorting));

        validateObjectTypes(objectTypes);

        String indexName;

        // search unconstrained by object type
        if (objectTypes.isEmpty()) {
            indexName = getAnyIndexPattern();
        }
        // search constrained by object types
        else {
            final List<String> rr = new LinkedList<>();
            for (final String type: objectTypes) {
                rr.add(checkIndex(type));
            }
            indexName = String.join(",", rr);
        }
        
        if (matchFilter.isExcludeSubObjects()) {
            indexName += EXCLUDE_SUB_OJBS_URL_SUFFIX;
        }

        final String urlPath = "/" + indexName + "/" + getDataTableName() + "/_search";
        final Response resp = makeRequestNoConflict("GET", urlPath, ImmutableMap.copyOf(doc));

        @SuppressWarnings("unchecked")
        final Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        ret.guids = new LinkedHashSet<>();
        @SuppressWarnings("unchecked")
        final Map<String, Object> hitMap = (Map<String, Object>) data.get("hits");
        ret.total = (Integer)hitMap.get("total");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hitMap.get("hits");
        if (loadObjects) {
            ret.objects = new ArrayList<>();
        }
        for (Map<String, Object> hit : hitList) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> obj = (Map<String, Object>) hit.get("_source");
            @SuppressWarnings("unchecked")
            final Map<String, List<String>> highlightRes =
                    (Map<String, List<String>>) hit.get("highlight");
            final String guidText = (String)obj.get("guid");
            ret.guids.add(new GUID(guidText));
            if (loadObjects) {
                ret.objects.add(buildObjectData(obj, highlightRes, pp));
            }
        }
        return ret;
    }

    private List<Object> createSortQuery(final List<SortingRule> sorting) {
        final List<Object> sort = new ArrayList<>();
        for (final SortingRule sr : sorting) {
            final Map<String, String> order = ImmutableMap.of
                    ("order", sr.isAscending() ? "asc" : "desc");
            final Map<String, Object> sortWrapper;
            if (sr.isKeyProperty()) {
                sortWrapper = ImmutableMap.of(getKeyProperty(sr.getKeyProperty().get()), order);
            } else {
                if (!READABLE_NAMES.inverse().containsKey(sr.getStandardProperty().get())) {
                    throw new IllegalArgumentException("Unknown object property " +
                            sr.getStandardProperty().get());
                }
                sortWrapper = ImmutableMap.of(
                        READABLE_NAMES.inverse().get(sr.getStandardProperty().get()), order);
            }
            sort.add(sortWrapper);
        }
        return sort;
    }

    private String getKeyProperty(final String keyName) {
        return "key." + keyName;
    }

    public Set<String> listIndeces() throws IOException {
        Set<String> ret = new TreeSet<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> data = UObject.getMapper().readValue(
                makeRequestNoConflict("GET", "/_aliases", null).getEntity().getContent(), Map.class);
        ret.addAll(data.keySet());
        return ret;
    }
    
    public Response deleteIndex(String indexName) throws IOException {
        return makeRequestNoConflict("DELETE", "/" + indexName, null);
    }
    
    public Response refreshIndex(String indexName) throws IOException {
        return makeRequestNoConflict("POST", "/" + indexName + "/_refresh", null);
    }
    
    /** Refresh the elasticsearch index, where the index prefix is set by
     * {@link #setIndexNamePrefix(String)}. Primarily used for testing.
     * @param rule the parsing rules that describes the index.
     * @return the response from the ElasticSearch server.
     * @throws IOException if an IO error occurs.
     */
    public Response refreshIndexByType(final ObjectTypeParsingRules rule)
            throws IOException {
        return refreshIndex(toIndexString(rule));
    }

    private RestClient getRestClient() {
        if (restClient == null) {
            RestClientBuilder restClientBld = RestClient.builder(esHost);
            restClientBld.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                    return requestConfigBuilder.setConnectTimeout(10000)
                            .setSocketTimeout(10 * 60 * 1000);
                }
            }).setMaxRetryTimeoutMillis(10 * 60 * 1000);
            List<Header> headers = new ArrayList<>();
            headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
            //headers.add(new BasicHeader("Role", "Read"));
            restClientBld.setDefaultHeaders(headers.toArray(new Header[headers.size()]));
            if (esUser != null) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, 
                        new UsernamePasswordCredentials(esUser, esPassword));
                restClientBld.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                        return hacb.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            }
            restClient = restClientBld.build();
        }
        return restClient;
    }

    public Response makeRequestNoConflict(
            final String reqType,
            final String urlPath,
            final Map<String, ?> doc) 
            throws IOException {
        try {
            return makeRequest(reqType, urlPath, doc);
        } catch (IndexingConflictException e) {
            // this is really difficult to test, and so is not
            throw new IOException(
                    "This operation is not expected to result in a conflict, yet it occurred: " +
                    e.getMessage(), e);
        }
    }
    
    public Response makeRequest(
            final String reqType,
            final String urlPath,
            final Map<String, ?> doc) 
            throws IOException, IndexingConflictException {
        return makeRequest(reqType, urlPath, doc, Collections.emptyMap());
    }
    
    public Response makeRequestBulk(
            final String reqType,
            final String indexName,
            final File jsonData) 
            throws IOException, IndexingConflictException {
        try (InputStream is = new FileInputStream(jsonData)) {
            return makeRequest(reqType, "/" + indexName + "/_bulk", Collections.emptyMap(),
                    new InputStreamEntity(is));
        }
    }
    
    private Response makeRequest(
            final String reqType,
            final String urlPath,
            final Map<String, ?> doc, 
            final Map<String, String> attributes)
            throws IOException, IndexingConflictException {
        return makeRequest(reqType, urlPath, attributes, doc == null ? null : stringEntity(
                UObject.transformObjectToString(doc)));
    }

    private Response makeRequest(
            final String reqType,
            final String urlPath,
            final Map<String, String> attributes,
            final HttpEntity body)
            throws IOException, IndexingConflictException {
        try {
            return getRestClient().performRequest(reqType, urlPath, attributes, body);
        } catch (ResponseException re) {
            if (re.getResponse().getStatusLine().getStatusCode() == 409) {
                // this is really difficult to test, and so is not
                throw new IndexingConflictException(re.getMessage(), re);
            }
            throw new IOException(re.getMessage(), re);
        }
    }
    
    private StringEntity stringEntity(final String string) {
        try {
            return new StringEntity(string);
            /* based on reading the code here:
             * https://svn.apache.org/repos/asf/httpcomponents/httpcore/branches/4.4.x/httpcore/src/main/java/org/apache/http/entity/StringEntity.java
             * this SE constructor never actually throws UEE. It looks like a typo. Hence ignore.
             */
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                    "This error can't happen, so you're obviously not seeing it.", e);
        }
    }

    private String getEsType(boolean fullText, Optional<String> keywordType) {
        if (fullText) {
            return "text";
        }
        if (keywordType.get().equals("string")) {
            return "keyword"; //TODO CODE why? why? why? 
        }
        return keywordType.get();
    }
    
    private String getDataTableName() {
        return "data";
    }
    
    private String getAccessTableName() {
        return "access";
    }
    
    private Map<String, Object> createAccessTable() {

        // props = {"properties": {},
        //          "pguid": {"type": "keyword"},
        //          "prefix": {"type": "keyword"},
        //          "version": {"type": "integer"},
        //          "lastin": {"type": "integer"},
        //          "groups": {"type": "integer"},
        //          "extpub": {"type": "integer"}}
        Map<String, Object> props = new LinkedHashMap<>();

        Map<String, Object> tmp = ImmutableMap.of("type", "keyword");
        props.put("pguid", tmp);

        tmp = ImmutableMap.of("type", "keyword");
        props.put("prefix", tmp);

        tmp = ImmutableMap.of("type", "integer");
        props.put("version", tmp);

        tmp = ImmutableMap.of("type", "integer");
        props.put("lastin", tmp);

        tmp = ImmutableMap.of("type", "integer");
        props.put("groups", tmp);

        // List of external workspaces containing DataPalette pointing to this object
        // This is the way to check how many public workspaces (external comparing to
        // home workspace of an object) have DataPalettes referencing given object (and
        // version). If this number is 0 then object+version is not visible as public
        // through DataPalettes. If it's >0 (which is the same as existence of keywords
        // in 'extpub') then it's visible.

        tmp = ImmutableMap.of("type", "integer");
        props.put("extpub", tmp);

        // mappings = {"access": {}}
        Map<String, Object> table = ImmutableMap.of("properties", ImmutableMap.copyOf(props));

        String tableName = getAccessTableName();
        Map<String, Object> mappings = ImmutableMap.of(tableName, table);

        return mappings;
    }
    
    private void createTables(String indexName, List<IndexingRules> indexingRules) throws IOException {

        Map<String, Object> props = new LinkedHashMap<>();
        final Map<String, Object> keyword = ImmutableMap.of("type", "keyword");
        final ImmutableMap<String, String> integer = ImmutableMap.of("type", "integer");
        final ImmutableMap<String, Object> bool = ImmutableMap.of("type", "boolean");

        props.put("guid", keyword);

        props.put(SEARCH_OBJ_TYPE, keyword);
        props.put(SEARCH_OBJ_TYPE_VER, integer);
        
        props.put(SOURCE_TAGS, keyword);

        props.put(OBJ_NAME, ImmutableMap.of("type", "text"));

        props.put(OBJ_CREATOR, keyword);
        props.put(OBJ_COPIER, keyword);
        props.put(OBJ_PROV_MODULE, keyword);
        props.put(OBJ_PROV_METHOD, keyword);
        props.put(OBJ_PROV_MODULE_VERSION, keyword);
        props.put(OBJ_PROV_COMMIT_HASH, keyword);
        props.put(OBJ_MD5, keyword);

        props.put(OBJ_TIMESTAMP, ImmutableMap.of("type", "date"));
        
        props.put(OBJ_PREFIX, keyword);
        props.put(OBJ_STORAGE_CODE, keyword);
        props.put(OBJ_ACCESS_GROUP_ID, integer);
        props.put(OBJ_VERSION, integer);

        props.put(OBJ_IS_LAST, bool);
        props.put(OBJ_PUBLIC, bool);
        props.put(OBJ_SHARED, bool);

        props.put("ojson", ImmutableMap.of(
                "type", "keyword",
                "index", false,
                "doc_values", false));

        props.put("pjson", ImmutableMap.of(
                "type", "keyword",
                "index", false,
                "doc_values", false));
        
        
        for (IndexingRules rules : indexingRules) {
            String propName = getKeyProperty(rules.getKeyName());
            String propType = getEsType(rules.isFullText(), rules.getKeywordType());
            props.put(propName, ImmutableMap.of("type", propType));
        }

        // table = {"data": {},
        //          "_parent": { "type": "access"},
        //                       "properties": {"guid": {"type": "keyword"},
        //                                     {"otype": {"type": "keyword"},
        //                                     {"otypever": {"type": "integer"},
        //                                     {"oname": {"type": "text"},
        //                                     {"creator": {"type": "keyword"},
        //                                     ...}}}
        Map<String, Object> table = new LinkedHashMap<>();


        table.put("_parent", ImmutableMap.of("type", getAccessTableName()));
        table.put("properties", ImmutableMap.copyOf(props));

        // Access (parent)
        Map<String, Object> mappings = new LinkedHashMap<>(createAccessTable());

        String tableName = getDataTableName();
        mappings.put(tableName, table);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("mappings", mappings);

        makeRequestNoConflict("PUT", "/" + indexName, doc);
    }
    
    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
    }
}
