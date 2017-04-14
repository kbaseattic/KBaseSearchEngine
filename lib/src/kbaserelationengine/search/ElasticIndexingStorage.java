package kbaserelationengine.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import us.kbase.common.service.UObject;

public class ElasticIndexingStorage implements IndexingStorage {
    private HttpHost esHost;
    private String esIndexName;
    private boolean mergeTypes = false;
    private boolean skipFullJson = false;

    public ElasticIndexingStorage(HttpHost esHost, String esIndexName) throws IOException {
        this.esHost = esHost;
        this.esIndexName = esIndexName;
        makeRequest("PUT", "/" + esIndexName, null);
    }
    
    public HttpHost getEsHost() {
        return esHost;
    }
    
    public String getEsIndexName() {
        return esIndexName;
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
    
    @Override
    public void indexObjects(String objectType, Map<GUID, String> idToJsonValues, 
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        
    }
    
    @Override
    public void indexObject(GUID id, String objectType, String valueJson,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        checkSchema(objectType, indexingRules);
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
        try (JsonParser jp = UObject.getMapper().getFactory().createParser(valueJson)) {
            collector.mapKeys(root, jp, consumer);
        }
        GUID parentId = new GUID(id.getStorageCode(), id.getAccessGroupId(), 
                id.getAccessGroupObjectId(), id.getVersion(), null, null);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.putAll(indexPart);
        doc.put("_guid", id.toString());
        doc.put("_pguid", parentId.toString());
        doc.put("_otype", objectType);
        if (!skipFullJson) {
            doc.put("_ojson", valueJson);
        }
        // Save new doc with auto-incremented ID
        // TODO: save many docs at once in bulk mode
        makeRequest("POST", "/" + esIndexName + "/" + getTableName(objectType) + "/", doc);
        shareObject(objectType, parentId, id.getAccessGroupId());
    }
    
    @Override
    public void shareObject(String objectType, GUID id, int accessGroupId) throws IOException {
        //throw new IllegalStateException("Unsupported");
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    public List<Object> getObjectsByIds(String objectType, Set<GUID> ids) throws IOException {
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
        String urlPath = "/" + esIndexName + "/" + getTableName(objectType) + "/_search";
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
    
    @Override
    public Set<GUID> searchIdsByText(String objectType, String text, List<SortingRule> sorting,
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        return queryIds(objectType, "match", "_all", text, accessGroupIds, isAdmin);
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    public Set<GUID> queryIds(String objectType, String queryType, String keyName, 
            Object keyValue, Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        Map<String, Object> match = new LinkedHashMap<String, Object>() {{
            put(keyName, keyValue);
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put(queryType, match);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("_guid"));
        }};
        String urlPath = "/" + esIndexName + "/" + getTableName(objectType) + "/_search";
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
    public List<String> listIndeces() throws IOException {
        List<String> ret = new ArrayList<>();
        Map<String, Object> data = UObject.getMapper().readValue(
                makeRequest("GET", "/_all/_settings", null).getEntity().getContent(), Map.class);
        ret.addAll(data.keySet());
        return ret;
    }
    
    public Response deleteIndex(String indexName) throws IOException {
        if (indexName == null) {
            indexName = esIndexName;
        }
        return makeRequest("DElETE", "/" + indexName, null);
    }
    
    public Response refreshIndex(String indexName) throws IOException {
        if (indexName == null) {
            indexName = esIndexName;
        }
        return makeRequest("POST", "/" + indexName + "/_refresh", null);
    }
    
    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) 
            throws IOException {
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
            return restClient.performRequest(reqType, urlPath, Collections.<String, String>emptyMap(),
                    body);
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
    
    private String getTableName(String type) {
        return mergeTypes ? "all" : type;
    }
    
    @SuppressWarnings("serial")
    private void checkSchema(String type, List<IndexingRules> indexingRules) throws IOException {
        String tableName = getTableName(type);
        Map<String, Object> doc = new LinkedHashMap<>();
        Map<String, Object> table = new LinkedHashMap<>();
        doc.put(tableName, table);
        Map<String, Object> props = new LinkedHashMap<>();
        table.put("properties", props);
        props.put("_guid", new LinkedHashMap<String, Object>() {{
            put("type", "keyword");
        }});
        props.put("_pguid", new LinkedHashMap<String, Object>() {{
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
        makeRequest("PUT", "/" + esIndexName + "/" + tableName + "/_mapping", doc);
    }
}
