package kbaserelationengine.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ValueCollectingNode;
import kbaserelationengine.parse.ValueCollector;
import kbaserelationengine.parse.ValueConsumer;
import kbaserelationengine.system.IndexingRules;
import us.kbase.common.service.Tuple2;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorage implements IndexingStorage {
    private HttpHost esHost;
    private String esUser;
    private String esPassword;
    private String indexNamePrefix;
    private boolean mergeTypes = false;
    private boolean skipFullJson = false;
    private Map<String, String> typeToIndex = new LinkedHashMap<>();
    private RestClient restClient = null;
    private File tempDir;
    
    public static final int PUBLIC_ACCESS_GROUP = -1;
    public static final int ADMIN_ACCESS_GROUP = -2;

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
    
    public boolean isMergeTypes() {
        return mergeTypes;
    }
    
    public void setMergeTypes(boolean mergeTypes) {
        this.mergeTypes = mergeTypes;
    }
    
    public boolean isSkipFullJson() {
        return skipFullJson;
    }
    
    public void setSkipFullJson(boolean skipFullJson) {
        this.skipFullJson = skipFullJson;
    }
    
    public String getIndex(String objectType) throws IOException {
        return checkIndex(objectType, null);
    }
    
    private String checkIndex(String objectType, List<IndexingRules> indexingRules) 
            throws IOException {
        String key = mergeTypes ? "all_types" : objectType;
        String ret = typeToIndex.get(key);
        if (ret == null) {
            ret = (indexNamePrefix + key).toLowerCase();
            if (!listIndeces().contains(ret)) {
                if (indexingRules == null) {
                    throw new IOException("Index wasn't created for type " + objectType);
                }
                System.out.println("Creating Elasticsearch index: " + ret);
                createTables(ret, indexingRules);
            }
            typeToIndex.put(key, ret);
        }
        return ret;
    }
    
    @SuppressWarnings("serial")
    @Override
    public void indexObjects(String objectType, String objectName, long timestamp, 
            String parentJsonValue, Map<String, String> metadata, Map<GUID, String> idToJsonValues,
            boolean isPublic, List<IndexingRules> indexingRules) 
                    throws IOException, ObjectParseException {
        String indexName = checkIndex(objectType, indexingRules);
        Set<GUID> parentGuids = new LinkedHashSet<>();
        Map<GUID, GUID> guidToParentGuid = new LinkedHashMap<>();
        for (GUID id : idToJsonValues.keySet()) {
            GUID parentGuid = new GUID(id.getStorageCode(), id.getAccessGroupId(), 
                    id.getAccessGroupObjectId(), id.getVersion(), null, null);
            parentGuids.add(parentGuid);
            guidToParentGuid.put(id, parentGuid);
        }
        File tempFile = File.createTempFile("es_bulk_", ".json", tempDir);
        try {
            PrintWriter pw = new PrintWriter(tempFile);
            Map<GUID, String> parentGuidToEsId = checkParentDoc(indexName, parentGuids, isPublic);
            for (GUID id : idToJsonValues.keySet()) {
                GUID parentGuid = guidToParentGuid.get(id);
                String esParentId = parentGuidToEsId.get(parentGuid);
                String json = idToJsonValues.get(id);
                Map<String, Object> doc = convertObject(id, objectType, json, objectName, 
                        timestamp, parentJsonValue, metadata, indexingRules);
                Map<String, Object> index = new LinkedHashMap<String, Object>() {{
                    put("_index", indexName);
                    put("_type", getDataTableName());
                    put("parent", esParentId);
                }};
                Map<String, Object> header = new LinkedHashMap<String, Object>() {{
                    put("index", index);
                }};
                pw.println(UObject.transformObjectToString(header));
                pw.println(UObject.transformObjectToString(doc));
            }
            pw.close();
            makeBulkRequest("POST", indexName, tempFile);
        } finally {
            tempFile.delete();
        }
        refreshIndex(indexName);
    }
    
    private Map<String, Object> convertObject(GUID id, String objectType, String json, 
            String objectName, long timestamp, String parentJson, Map<String, String> metadata,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        Map<String, List<Object>> indexPart = new LinkedHashMap<>();
        ValueConsumer<List<IndexingRules>> consumer = new ValueConsumer<List<IndexingRules>>() {
            @Override
            public void addValue(List<IndexingRules> rulesList, Object value) {
                for (IndexingRules rules : rulesList) {
                    Object valueFinal = value;
                    String key;
                    if (mergeTypes) {
                        key = getKeyProperty("all");
                    } else {
                        key = getKeyProperty(getKeyName(rules));
                    }
                    List<Object> values = indexPart.get(key);
                    if (values == null) {
                        values = new ArrayList<>();
                        indexPart.put(key, values);
                    }
                    if (rules.getTransform() != null) {
                        valueFinal = transform(valueFinal, rules.getTransform());
                    }
                    addOrAddAll(valueFinal, values);
                }
            }
        };
        // Sub-objects
        extractIndexingPart(json, false, indexingRules, consumer);
        // Parent
        if (parentJson != null) {
            extractIndexingPart(parentJson, true, indexingRules, consumer);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.putAll(indexPart);
        doc.put("guid", id.toString());
        doc.put("otype", objectType);
        doc.put("oname", objectName);
        doc.put("timestamp", timestamp);
        if (!skipFullJson) {
            doc.put("ojson", json);
            doc.put("pjson", parentJson);
        }
        return doc;
    }

    @SuppressWarnings("unchecked")
    private void addOrAddAll(Object valueFinal, List<Object> values) {
        if (valueFinal != null) {
            if (valueFinal instanceof List) {
                values.addAll((List<Object>)valueFinal);
            } else {
                values.add(valueFinal);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object transform(Object value, String transform) {
        String retProp = null;
        if (transform.contains(".")) {
            String[] parts = transform.split(Pattern.quote("."));
            transform = parts[0];
            retProp = parts[1];
        }
        switch (transform) {
        case "location":
            List<List<Object>> loc = (List<List<Object>>)value;
            Map<String, Object> retLoc = new LinkedHashMap<>();
            retLoc.put("contig_id", loc.get(0).get(0));
            String strand = (String)loc.get(0).get(2);
            retLoc.put("strand", strand);
            int start = (Integer)loc.get(0).get(1);
            int len = (Integer)loc.get(0).get(3);
            retLoc.put("length", len);
            retLoc.put("start", strand.equals("+") ? start : (start - len + 1));
            retLoc.put("stop", strand.equals("+") ? (start + len - 1) : start);
            if (retProp == null) {
                return retLoc;
            }
            return retLoc.get(retProp);
        case "values":
            if (value == null) {
                return null;
            }
            if (value instanceof List) {
                List<Object> input = (List<Object>)value;
                List<Object> ret = new ArrayList<>();
                for (Object item : input) {
                    addOrAddAll(transform(item, transform), ret);
                }
                return ret;
            }
            if (value instanceof Map) {
                Map<String, Object> input = (Map<String, Object>)value;
                List<Object> ret = new ArrayList<>();
                for (Object item : input.values()) {
                    addOrAddAll(transform(item, transform), ret);
                }
                return ret;
            }
            return String.valueOf(value);
        case "string":
            return String.valueOf(value);
        case "integer":
            return Integer.parseInt(String.valueOf(value));
        default:
            throw new IllegalStateException("Unsupported transformation type: " + transform);
        }
    }

    private void extractIndexingPart(String json, boolean fromParent,
            List<IndexingRules> indexingRules, ValueConsumer<List<IndexingRules>> consumer)
            throws IOException, ObjectParseException, JsonParseException {
        Map<ObjectJsonPath, List<IndexingRules>> pathToRules = new LinkedHashMap<>();
        for (IndexingRules rules : indexingRules) {
            if (rules.isFromParent() != fromParent) {
                continue;
            }
            if (rules.isFullText() || rules.getKeywordType() != null) {
                List<IndexingRules> rulesList = pathToRules.get(rules.getPath());
                if (rulesList == null) {
                    rulesList = new ArrayList<>();
                    pathToRules.put(rules.getPath(), rulesList);
                }
                rulesList.add(rules);
            }
        }
        ValueCollectingNode<List<IndexingRules>> root = new ValueCollectingNode<>();
        for (ObjectJsonPath path : pathToRules.keySet()) {
            root.addPath(path, pathToRules.get(path));
        }
        ValueCollector<List<IndexingRules>> collector = new ValueCollector<List<IndexingRules>>();
        try (JsonParser jp = UObject.getMapper().getFactory().createParser(json)) {
            collector.mapKeys(root, jp, consumer);
        }
    }
    
    @Override
    public void indexObject(GUID id, String objectType, String json, String objectName,
            long timestamp, String parentJsonValue, Map<String, String> metadata, boolean isPublic,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        String indexName = checkIndex(objectType, indexingRules);
        GUID parentGUID = new GUID(id.getStorageCode(), id.getAccessGroupId(), 
                id.getAccessGroupObjectId(), id.getVersion(), null, null);
        String esParentId = checkParentDoc(indexName, 
                new HashSet<>(Arrays.asList(parentGUID)), isPublic).get(parentGUID);
        Map<String, Object> doc = convertObject(id, objectType, json, objectName, timestamp, 
                parentJsonValue, metadata, indexingRules);
        makeRequest("POST", "/" + indexName + "/" + getDataTableName() + "/", doc, Arrays.asList(
                new Tuple2<String, String>().withE1("parent").withE2(esParentId)));
        refreshIndex(indexName);
    }
    
    @Override
    public void flushIndexing(String objectType) throws IOException {
        refreshIndex(getIndex(objectType));
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    private Map<GUID, String> lookupParentDocIds(String indexName, Set<GUID> guids) throws IOException {
        Map<String, Object> terms = new LinkedHashMap<String, Object>() {{
            put("pguid", guids.stream().map(u -> u.toString()).collect(Collectors.toList()));
        }};
        Map<String, Object> filter = new LinkedHashMap<String, Object>() {{
            put("terms", terms);
        }};
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("filter", Arrays.asList(filter));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
        }};
        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<GUID, String> ret = new LinkedHashMap<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            String id = (String)hit.get("_id");
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            GUID guid = new GUID((String)obj.get("pguid"));
            ret.put(guid, id);
        }
        return ret;
    }

    @SuppressWarnings({ "serial", "unchecked" })
    public Map<String, Set<GUID>> groupIdsByIndex(Set<GUID> ids) throws IOException {
        Map<String, Object> terms = new LinkedHashMap<String, Object>() {{
            put("guid", ids.stream().map(u -> u.toString()).collect(Collectors.toList()));
        }};
        Map<String, Object> filter = new LinkedHashMap<String, Object>() {{
            put("terms", terms);
        }};
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("filter", filter);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("guid"));
        }};
        String urlPath = "/_all/" + getDataTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<String, Set<GUID>> ret = new LinkedHashMap<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            String indexName = (String)hit.get("_index");
            Set<GUID> retSet = ret.get(indexName);
            if (retSet == null) {
                retSet = new LinkedHashSet<>();
                ret.put(indexName, retSet);
            }
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            GUID guid = new GUID((String)obj.get("guid"));
            retSet.add(guid);
        }
        return ret;
    }

    public Map<String, AccessInfo> lookupParentDocsByPrefix(GUID guid) throws IOException {
        return lookupParentDocsByPrefix(null, guid);
    }

    @SuppressWarnings({ "serial", "unchecked" })
    public Map<String, AccessInfo> lookupParentDocsByPrefix(String indexName,
            GUID guid) throws IOException {
        if (indexName == null) {
            indexName = "_all";
        }
        String prefix = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), null, null, null).toString();
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("filter", Arrays.asList(
                    createFilter("term", "prefix", prefix)
                    ));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
        }};
        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<String, AccessInfo> ret = new LinkedHashMap<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            String index = (String)hit.get("_index");
            String type = (String)hit.get("_type");
            String id = (String)hit.get("_id");
            ret.put(index + "/" + type + "/" + id, AccessInfo.fromMap(obj));
        }
        return ret;
    }

    @SuppressWarnings("serial")
    private Map<String, Object> createFilter(String queryType, String keyName, Object value) {
        Map<String, Object> term = new LinkedHashMap<String, Object>() {{
            put(keyName, value);
        }};
        return new LinkedHashMap<String, Object>() {{
            put(queryType, term);
        }};
    }
    
    @SuppressWarnings("unchecked")
    public Map<GUID, String> checkParentDoc(String indexName, Set<GUID> parentGUIDs, 
            boolean isPublic) throws IOException {
        boolean changed = false;
        Map<GUID, String> ret = lookupParentDocIds(indexName, parentGUIDs);
        for (GUID parentGUID : parentGUIDs) {
            if (ret.containsKey(parentGUID)) {
                continue;
            }
            String prefix = new GUID(parentGUID.getStorageCode(), parentGUID.getAccessGroupId(),
                    parentGUID.getAccessGroupObjectId(), null, null, null).toString();
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
            doc.put("lastin", accessGroupIds);
            doc.put("groups", accessGroupIds);
            Response resp = makeRequest("POST", "/" + indexName + "/" + getAccessTableName() + "/", 
                    doc);
            Map<String, Object> data = UObject.getMapper().readValue(
                    resp.getEntity().getContent(), Map.class);
            ret.put(parentGUID, (String)data.get("_id"));
            changed = true;
            removeAccessGroupForOtherVersions(indexName, parentGUID, 
                    accessGroupIds.toArray(new Integer[accessGroupIds.size()]));
        }
        if (changed) {
            refreshIndex(indexName);
        }
        return ret;
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    private boolean removeAccessGroupForOtherVersions(String indexName, GUID guid, 
            Integer... accessGroupIds) throws IOException {
        if (indexName == null) {
            indexName = "_all";
        }
        String prefix = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), null, null, null).toString();
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", Arrays.asList(createFilter("term", "prefix", prefix),
                    createFilter("terms", "lastin", accessGroupIds)));
            put("must_not", Arrays.asList(createFilter("term", "version", guid.getVersion())));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        StringBuilder inline = new StringBuilder();
        for (int accessGroupId : accessGroupIds) {
            inline.append(""+ 
                    "if (ctx._source.lastin.indexOf(" + accessGroupId + ") >= 0) {\n" +
                    "  ctx._source.lastin.remove(ctx._source.lastin.indexOf(" + accessGroupId + "));\n" +
                    "}\n"
                    );
        }
        Map<String, Object> script = new LinkedHashMap<String, Object>() {{
            put("inline", inline.toString());
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("script", script);
        }};
        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    @SuppressWarnings({ "serial", "unchecked" })
    private boolean removeAccessGroupForVersion(String indexName, GUID guid, 
            int accessGroupId) throws IOException {
        if (indexName == null) {
            indexName = "_all";
        }
        // Check that we work with other than physical access group this object exists in.
        boolean fromAllGroups = accessGroupId != guid.getAccessGroupId();
        String pguid = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), guid.getVersion(), null, null).toString();
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", Arrays.asList(createFilter("term", "pguid", pguid),
                    createFilter("term", "lastin", accessGroupId)));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> script = new LinkedHashMap<String, Object>() {{
            put("inline", "" + 
                    "ctx._source.lastin.remove(ctx._source.lastin.indexOf(" + accessGroupId + ")); " +
                    (fromAllGroups ? (
                    "int pos = ctx._source.groups.indexOf(" + accessGroupId + "); " +
                    "if (pos >= 0) {" +
                    "  ctx._source.groups.remove(pos); " +
                    "}") : ""));
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("script", script);
        }};
        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    @SuppressWarnings({ "serial", "unchecked" })
    private boolean addAccessGroupForVersion(String indexName, GUID guid, 
            int accessGroupId) throws IOException {
        // Here we try to update 'lastin' parameter in access parent. The idea is we need only one
        // version among other parent with the same prefix to contain any particular access group.
        if (indexName == null) {
            indexName = "_all";
        }
        String prefix = new GUID(guid.getStorageCode(), guid.getAccessGroupId(),
                guid.getAccessGroupObjectId(), null, null, null).toString();
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", Arrays.asList(createFilter("term", "prefix", prefix),
                    createFilter("term", "version", guid.getVersion())));
            put("must_not", Arrays.asList(createFilter("term", "lastin", accessGroupId)));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> script = new LinkedHashMap<String, Object>() {{
            put("inline", "" + 
                    "ctx._source.lastin.add(" + accessGroupId + "); " +
                    "if (!ctx._source.groups.contains(" + accessGroupId + ")) {" +
                    "  ctx._source.groups.add(" + accessGroupId + ");" + 
                    "}");
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("script", script);
        }};
        String urlPath = "/" + indexName + "/" + getAccessTableName() + "/_update_by_query";
        Response resp = makeRequest("POST", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        return (Integer)data.get("updated") > 0;
    }

    @Override
    public void shareObjects(Set<GUID> guids, int accessGroupId) throws IOException {
        Map<String, Set<GUID>> indexToGuids = groupIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (addAccessGroupForVersion(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
                if (removeAccessGroupForOtherVersions(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
        }
    }
    
    @Override
    public void unshareObjects(Set<GUID> guids, int accessGroupId) throws IOException {
        Map<String, Set<GUID>> indexToGuids = groupIdsByIndex(guids);
        for (String indexName : indexToGuids.keySet()) {
            boolean needRefresh = false;
            for (GUID guid : indexToGuids.get(indexName)) {
                if (removeAccessGroupForVersion(indexName, guid, accessGroupId)) {
                    needRefresh = true;
                }
            }
            if (needRefresh) {
                refreshIndex(indexName);
            }
        }
    }
    
    @Override
    public void publishObjects(Set<GUID> guids) throws IOException {
        shareObjects(guids, PUBLIC_ACCESS_GROUP);
    }
    
    @Override
    public void unpublishObjects(Set<GUID> guids) throws IOException {
        unshareObjects(guids, PUBLIC_ACCESS_GROUP);
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    public List<ObjectData> getObjectsByIds(Set<GUID> ids) throws IOException {
        Map<String, Object> terms = new LinkedHashMap<String, Object>() {{
            put("guid", ids.stream().map(u -> u.toString()).collect(Collectors.toList()));
        }};
        Map<String, Object> filter = new LinkedHashMap<String, Object>() {{
            put("terms", terms);
        }};
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("filter", filter);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            //put("_source", Arrays.asList("ojson"));
        }};
        String urlPath = "/_all/" + getDataTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        List<ObjectData> ret = new ArrayList<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            ObjectData item = new ObjectData();
            item.guid = new GUID((String)obj.get("guid"));
            item.objectName = (String)obj.get("oname");
            Object dateProp = obj.get("timestamp");
            item.timestamp = (dateProp instanceof Long) ? (Long)dateProp : 
                Long.parseLong(String.valueOf(dateProp));
            item.data = UObject.transformStringToObject((String)obj.get("ojson"), Object.class);
            String pjson = (String)obj.get("pjson");
            if (pjson != null) {
                item.parentData = UObject.transformStringToObject(pjson, Object.class);
                item.parentGuid = new GUID(item.guid.getStorageCode(), item.guid.getAccessGroupId(),
                        item.guid.getAccessGroupObjectId(), item.guid.getVersion(), null, null);
            }
            Map<String, String> keyProps = new LinkedHashMap<>();
            for (String key : obj.keySet()) {
                if (key.startsWith("key.")) {
                    Object objValue = obj.get(key);
                    String textValue = null;
                    if (objValue instanceof List) {
                        textValue = ((List<Object>)objValue).stream().map(Object::toString)
                                .collect(Collectors.joining(", "));
                    } else {
                        textValue = String.valueOf(objValue);
                    }
                    keyProps.put(key.substring(4), textValue);
                }
            }
            item.keyProps = keyProps;
            ret.add(item);
        }
        return ret;
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    public Map<String, Integer> searchTypeByText(String text,
            AccessFilter accessFilter) throws IOException {
        Map<String, Object> match = new LinkedHashMap<String, Object>() {{
            put("_all", text);
        }};
        Map<String, Object> must1 = new LinkedHashMap<String, Object>() {{
            put("match", match);
        }};
        Map<String, Object> must2 = createAccessMustBlock(accessFilter);
        if (must2 == null) {
            return Collections.emptyMap();
        }
        List<Object> mustList = new ArrayList<>(Arrays.asList(must1, must2));
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", mustList);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> terms = new LinkedHashMap<String, Object>() {{
            put("field", "otype");
        }};
        Map<String, Object> agg = new LinkedHashMap<String, Object>() {{
            put("terms", terms);
        }};
        Map<String, Object> aggs = new LinkedHashMap<String, Object>() {{
            put("types", agg);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("aggregations", aggs);
            put("size", 0);
        }};
        String urlPath = "/_all/" + getDataTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<String, Object> aggMap = (Map<String, Object>)data.get("aggregations");
        Map<String, Object> typeMap = (Map<String, Object>)aggMap.get("types");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>)typeMap.get("buckets");
        Map<String, Integer> ret = new TreeMap<>();
        for (Map<String, Object> bucket : buckets) {
            String objType = (String)bucket.get("key");
            Integer count = (Integer)bucket.get("doc_count");
            ret.put(objType, count);
        }
        return ret;
    }
    
    @Override
    public Set<GUID> searchIdsByText(String objectType, String text, List<SortingRule> sorting,
            AccessFilter accessFilter) throws IOException {
        return queryIds(objectType, "match", "_all", text, sorting, accessFilter);
    }
    
    @SuppressWarnings("serial")
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
        String groupListProp = accessFilter.withAllHistory ? "groups" : "lastin";
        Map<String, Object> match2 = new LinkedHashMap<String, Object>() {{
            put(groupListProp, accessGroupIds);
        }};
        Map<String, Object> query2 = new LinkedHashMap<String, Object>() {{
            put("terms", match2);
        }};
        Map<String, Object> hasParent = new LinkedHashMap<String, Object>() {{
            put("parent_type", getAccessTableName());
            put("query", query2);
        }};
        return new LinkedHashMap<String, Object>() {{
            put("has_parent", hasParent);
        }};
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    public Set<GUID> queryIds(String objectType, String queryType, String keyName, 
            Object keyValue, List<SortingRule> sorting, 
            AccessFilter accessFilter) throws IOException {
        Map<String, Object> match1 = new LinkedHashMap<String, Object>() {{
            put(keyName, keyValue);
        }};
        Map<String, Object> must1 = new LinkedHashMap<String, Object>() {{
            put(queryType, match1);
        }};
        Map<String, Object> must2 = createAccessMustBlock(accessFilter);
        if (must2 == null) {
            return Collections.emptySet();
        }
        List<Object> mustList = new ArrayList<>(Arrays.asList(must1, must2));
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", mustList);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("guid"));
        }};
        if (sorting != null && sorting.size() > 0) {
            List<Object> sort = new ArrayList<>();
            doc.put("sort", sort);
            for (SortingRule sr : sorting) {
                Map<String, Object> sortOrder = new LinkedHashMap<String, Object>() {{
                    put("order", sr.ascending ? "asc" : "desc");
                }};
                sort.add(new LinkedHashMap<String, Object>() {{
                    put(getKeyProperty(sr.keyName), sortOrder);
                }});
            }
        }
        String indexName = objectType == null ? "_all" : getIndex(objectType);
        String urlPath = "/" + indexName + "/" + getDataTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Set<GUID> ret = new LinkedHashSet<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            String guidText = (String)obj.get("guid");
            ret.add(new GUID(guidText));
        }
        return ret;
    }

    @Override
    public Set<GUID> lookupIdsByKey(String objectType, String keyName, Object keyValue,
            AccessFilter accessFilter) throws IOException {
        return queryIds(objectType, "term", getKeyProperty(keyName), keyValue, null, 
                accessFilter);
    }
    
    private String getKeyName(IndexingRules rules) {
        return rules.getKeyName() != null ? rules.getKeyName():
            rules.getPath().getPathItems()[0];
    }
    
    private String getKeyProperty(String keyName) {
        return "key." + keyName;
    }

    @SuppressWarnings("unchecked")
    public Set<String> listIndeces() throws IOException {
        Set<String> ret = new TreeSet<>();
        Map<String, Object> data = UObject.getMapper().readValue(
                makeRequest("GET", "/_all/_settings", null).getEntity().getContent(), Map.class);
        ret.addAll(data.keySet());
        return ret;
    }
    
    public Response deleteIndex(String indexName) throws IOException {
        return makeRequest("DElETE", "/" + indexName, null);
    }
    
    public Response refreshIndex(String indexName) throws IOException {
        return makeRequest("POST", "/" + indexName + "/_refresh", null);
    }

    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) 
            throws IOException {
        return makeRequest(reqType, urlPath, doc, Collections.<String, String>emptyMap());
    }

    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc, 
            List<Tuple2<String, String>> attributes) throws IOException {
        return makeRequest(reqType, urlPath, doc, attributes.stream().collect(
                Collectors.toMap(u -> u.getE1(), u -> u.getE2())));
    }
    
    private RestClient getRestClient() {
        if (restClient == null) {
            RestClientBuilder restClientBld = RestClient.builder(esHost);
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
    
    private Response makeBulkRequest(String reqType, String indexName, File jsonData) 
            throws IOException {
        RestClient restClient = getRestClient();
        try (InputStream is = new FileInputStream(jsonData)) {
            InputStreamEntity body = new InputStreamEntity(is);
            Response response = restClient.performRequest(reqType, indexName + "/_bulk", 
                    Collections.emptyMap(), body);
            return response;
        }
    }
    
    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc, 
            Map<String, String> attributes) throws IOException {
        try {
            HttpEntity body = doc == null ? null : 
                new StringEntity(UObject.transformObjectToString(doc));
            RestClient restClient = getRestClient();  //restClientBld.build();
            return restClient.performRequest(reqType, urlPath, attributes, body);
        } catch (ResponseException ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }
    
    private String getEsType(boolean fullText, String keywordType) {
        if (fullText) {
            return "text";
        }
        if (keywordType == null || keywordType.equals("string")) {
            return "keyword";
        }
        return keywordType;
    }
    
    private String getDataTableName() {
        return "data";
    }
    
    private String getAccessTableName() {
        return "access";
    }
    
    @SuppressWarnings("serial")
    private void createAccessTable(String indexName, Map<String, Object> mappings) throws IOException {
        String tableName = getAccessTableName();
        Map<String, Object> table = new LinkedHashMap<>();
        mappings.put(tableName, table);
        Map<String, Object> props = new LinkedHashMap<>();
        table.put("properties", props);
        props.put("pguid", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("prefix", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("version", new LinkedHashMap<String, Object>() {{
            put("type", "integer");
        }});
        props.put("lastin", new LinkedHashMap<String, Object>() {{
            put("type", "integer");
        }});
        props.put("groups", new LinkedHashMap<String, Object>() {{
            put("type", "integer");
        }});
    }
    
    @SuppressWarnings("serial")
    private void createTables(String indexName, List<IndexingRules> indexingRules) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        Map<String, Object> mappings = new LinkedHashMap<>();
        doc.put("mappings", mappings);
        // Access (parent)
        createAccessTable(indexName, mappings);
        // Now data (child)
        String tableName = getDataTableName();
        Map<String, Object> table = new LinkedHashMap<>();
        mappings.put(tableName, table);
        table.put("_parent", new LinkedHashMap<String, Object>() {{
            put("type", getAccessTableName());
        }});
        Map<String, Object> props = new LinkedHashMap<>();
        table.put("properties", props);
        props.put("guid", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("otype", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("oname", new LinkedHashMap<String, Object>() {{
            put("type", "text");
        }});
        props.put("timestamp", new LinkedHashMap<String, Object>() {{
            put("type", "date");
        }});
        if (!skipFullJson) {
            props.put("ojson", new LinkedHashMap<String, Object>() {{
                put("type", "keyword");
                put("index", false);
            }});
            props.put("pjson", new LinkedHashMap<String, Object>() {{
                put("type", "keyword");
                put("index", false);
            }});
        }
        if (mergeTypes) {
            props.put("all", new LinkedHashMap<String, Object>() {{
                put("type", "text");
            }});
        } else {
            for (IndexingRules rules : indexingRules) {
                if (rules.getKeywordType() == null && !rules.isFullText()) {
                    continue;
                }
                String propName = getKeyProperty(getKeyName(rules));
                String propType = getEsType(rules.isFullText(), rules.getKeywordType());
                props.put(propName, new LinkedHashMap<String, Object>() {{
                    put("type", propType);
                }});
            }
        }
        makeRequest("PUT", "/" + indexName, doc);
    }
    
    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
    }
}
