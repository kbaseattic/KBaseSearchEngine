package kbaserelationengine.search;

import java.io.IOException;
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
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ValueCollectingNode;
import kbaserelationengine.parse.ValueCollector;
import kbaserelationengine.parse.ValueConsumer;
import kbaserelationengine.system.IndexingRules;
import us.kbase.common.service.Tuple2;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorage implements IndexingStorage {
    private HttpHost esHost;
    private String indexNamePrefix;
    private boolean mergeTypes = false;
    private boolean skipFullJson = false;
    private boolean allPublic = false;
    private Map<String, String> typeToIndex = new LinkedHashMap<>();

    public ElasticIndexingStorage(HttpHost esHost) throws IOException {
        this.esHost = esHost;
        this.indexNamePrefix = "";
    }
    
    public HttpHost getEsHost() {
        return esHost;
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
    
    public boolean isAllPublic() {
        return allPublic;
    }
    
    public void setAllPublic(boolean allPublic) {
        this.allPublic = allPublic;
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
                //makeRequest("PUT", "/" + ret, null);
                createTables(ret, indexingRules);
            }
            typeToIndex.put(key, ret);
        }
        return ret;
    }
    
    @Override
    public void indexObjects(String objectType, Map<GUID, String> idToJsonValues, 
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        for (GUID id : idToJsonValues.keySet()) {
            indexObject(id, objectType, idToJsonValues.get(id), indexingRules);
        }
    }
    
    @Override
    public void indexObject(GUID id, String objectType, String json,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        String indexName = checkIndex(objectType, indexingRules);
        Map<String, List<Object>> indexPart = new LinkedHashMap<>();
        ValueConsumer<IndexingRules> consumer = new ValueConsumer<IndexingRules>() {
            @Override
            public void addValue(IndexingRules rules, Object value) {
                String key;
                if (mergeTypes) {
                    key = "all";
                } else {
                    key = getKeyName(rules);
                }
                List<Object> values = indexPart.get(key);
                if (values == null) {
                    values = new ArrayList<>();
                    indexPart.put(key, values);
                }
                values.add(value);
            }
        };
        ValueCollectingNode<IndexingRules> root = new ValueCollectingNode<>();
        for (IndexingRules rules : indexingRules) {
            if (rules.isFullText() || rules.getKeywordType() != null) {
                root.addPath(rules.getPath(), rules);
            }
        }
        ValueCollector<IndexingRules> collector = new ValueCollector<IndexingRules>();
        try (JsonParser jp = UObject.getMapper().getFactory().createParser(json)) {
            collector.mapKeys(root, jp, consumer);
        }
        GUID parentGUID = new GUID(id.getStorageCode(), id.getAccessGroupId(), 
                id.getAccessGroupObjectId(), id.getVersion(), null, null);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.putAll(indexPart);
        doc.put("_guid", id.toString());
        String parentId = checkParentDoc(indexName, 
                new HashSet<>(Arrays.asList(parentGUID))).get(parentGUID);
        doc.put("_otype", objectType);
        if (!skipFullJson) {
            doc.put("_ojson", json);
        }
        // Save new doc with auto-incremented ID
        // TODO: save many docs at once in bulk mode
        makeRequest("POST", "/" + indexName + "/" + getDataTableName() + "/", doc, Arrays.asList(
                new Tuple2<String, String>().withE1("parent").withE2(parentId)));
        //shareObject(parentId, id.getAccessGroupId());
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
    public Map<String, AccessInfo> lookupParentDocsByPrefix(String indexName,
            GUID guid) throws IOException {
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
            String id = (String)hit.get("_id");
            ret.put(id, AccessInfo.fromMap(obj));
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
    public Map<GUID, String> checkParentDoc(String indexName, Set<GUID> parentGUIDs) 
            throws IOException {
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
            int accessGroupId = parentGUID.getAccessGroupId();
            doc.put("lastin", new LinkedHashSet<>(Arrays.asList(accessGroupId)));
            doc.put("groups", new LinkedHashSet<>(Arrays.asList(accessGroupId)));
            Response resp = makeRequest("POST", "/" + indexName + "/" + getAccessTableName() + "/", 
                    doc);
            Map<String, Object> data = UObject.getMapper().readValue(
                    resp.getEntity().getContent(), Map.class);
            ret.put(parentGUID, (String)data.get("_id"));
            changed = true;
        }
        if (changed) {
            refreshIndex(indexName);
        }
        return ret;
    }
    
    @Override
    public void shareObject(GUID id, int accessGroupId) throws IOException {
        //throw new IllegalStateException("Unsupported");
    }
    
    @Override
    public void unshareObject(GUID id, int accessGroupId) throws IOException {
        // TODO Auto-generated method stub
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    public List<Object> getObjectsByIds(Set<GUID> ids) throws IOException {
        Map<String, Object> match = new LinkedHashMap<String, Object>() {{
            put("_guid", ids.stream().map(u -> u.toString()).collect(Collectors.toList()));
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("terms", match);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("_ojson"));
        }};
        String urlPath = "/_all/" + getDataTableName() + "/_search";
        Response resp = makeRequest("GET", urlPath, doc);
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        List<Object> ret = new ArrayList<>();
        Map<String, Object> hitMap = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hitMap.get("hits");
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> obj = (Map<String, Object>)hit.get("_source");
            String jsonText = (String)obj.get("_ojson");
            ret.add(UObject.transformStringToObject(jsonText, Map.class));
        }
        return ret;
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    public Map<String, Integer> searchTypeByText(String text,
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        Map<String, Object> match = new LinkedHashMap<String, Object>() {{
            put("_all", text);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("match", match);
        }};
        Map<String, Object> terms = new LinkedHashMap<String, Object>() {{
            put("field", "_otype");
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
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        return queryIds(objectType, "match", "_all", text, accessGroupIds, isAdmin);
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    public Set<GUID> queryIds(String objectType, String queryType, String keyName, 
            Object keyValue, Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        Map<String, Object> match1 = new LinkedHashMap<String, Object>() {{
            put(keyName, keyValue);
        }};
        Map<String, Object> must1 = new LinkedHashMap<String, Object>() {{
            put(queryType, match1);
        }};
        List<Object> mustList = new ArrayList<>(Arrays.asList(must1));
        if (!(allPublic || isAdmin)) {
            if (accessGroupIds == null) {
                throw new NullPointerException("accessGroupIds parameter can't be null "
                        + "if not in admin mode");
            }
            if (accessGroupIds.isEmpty()) {
                return Collections.emptySet();
            }
            Map<String, Object> match2 = new LinkedHashMap<String, Object>() {{
                put("lastin", accessGroupIds);
            }};
            Map<String, Object> query2 = new LinkedHashMap<String, Object>() {{
                put("terms", match2);
            }};
            Map<String, Object> hasParent = new LinkedHashMap<String, Object>() {{
                put("parent_type", getAccessTableName());
                put("query", query2);
            }};
            mustList.add(new LinkedHashMap<String, Object>() {{
                put("has_parent", hasParent);
            }});
        }
        Map<String, Object> bool = new LinkedHashMap<String, Object>() {{
            put("must", mustList);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("bool", bool);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("_guid"));
        }};
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
            String guidText = (String)obj.get("_guid");
            ret.add(new GUID(guidText));
        }
        return ret;
    }

    @Override
    public Set<GUID> lookupIdsByKey(String objectType, String keyName, Object keyValue,
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        return queryIds(objectType, "term", keyName, keyValue, accessGroupIds, isAdmin);
    }
    
    private String getKeyName(IndexingRules rules) {
        return rules.getKeyName() != null ? rules.getKeyName():
            rules.getPath().getPathItems()[0];
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
    
    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc, 
            Map<String, String> attributes) throws IOException {
        try {
            RestClientBuilder restClientBld = RestClient.builder(esHost);
            List<Header> headers = new ArrayList<>();
            if (doc != null) {
                headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
            }
            //headers.add(new BasicHeader("Role", "Read"));
            restClientBld.setDefaultHeaders(headers.toArray(new Header[headers.size()]));
            /*
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, 
                    new UsernamePasswordCredentials("esadmin", "12345"));
            restClientBld.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                    return hacb.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
            */
            HttpEntity body = doc == null ? null : 
                new StringEntity(UObject.transformObjectToString(doc));
            RestClient restClient = restClientBld.build();
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
        props.put("_guid", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("_otype", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        if (!skipFullJson) {
            props.put("_ojson", new LinkedHashMap<String, Object>() {{
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
                String propName = getKeyName(rules);
                String propType = getEsType(rules.isFullText(), rules.getKeywordType());
                props.put(propName, new LinkedHashMap<String, Object>() {{
                    put("type", propType);
                }});
            }
        }
        makeRequest("PUT", "/" + indexName, doc);
    }
}
