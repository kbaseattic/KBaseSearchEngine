package kbaserelationengine.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ValueCollectingNode;
import kbaserelationengine.parse.ValueCollector;
import kbaserelationengine.parse.ValueConsumer;
import kbaserelationengine.system.IndexingRules;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorage implements IndexingStorage {
    private String esHost;
    private String esIndexName;
    private boolean mergeTypes = false;
    private boolean skipFullJson = false;

    public ElasticIndexingStorage(String esHost, String esIndexName) {
        this.esHost = esHost;
        this.esIndexName = esIndexName;
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
        makeRequest("POST", "/" + esIndexName + "/" + getTableName(objectType) + "/", doc);
        shareObject(parentId, id.getAccessGroupId());
    }
    
    @Override
    public void shareObject(GUID id, int accessGroupId) throws IOException {
        //throw new IllegalStateException("Unsupported");
    }
    
    @Override
    public List<Object> getObjectsByIds(Set<GUID> id) throws IOException {
        throw new IllegalStateException("Unsupported");
    }
    
    @Override
    public Set<GUID> searchIdsByText(String text, List<SortingRule> sorting, 
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        throw new IllegalStateException("Unsupported");
    }

    @Override
    public Set<GUID> lookupIdsByKey(String keyName, Object keyValue, 
            Set<Integer> accessGroupIds, boolean isAdmin) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    private String getKeyName(IndexingRules rules) {
        return rules.getKeyName() != null ? rules.getKeyName():
            rules.getPath().getPathItems()[0];
    }
    
    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) 
            throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost(esHost)).build();
        return restClient.performRequest(reqType, urlPath, Collections.<String, String>emptyMap(),
                new StringEntity(UObject.transformObjectToString(doc)));
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
        Map<String, Object> doc = new LinkedHashMap<>();
        Map<String, Object> mappings = new LinkedHashMap<>();
        doc.put("mappings", mappings);
        Map<String, Object> table = new LinkedHashMap<>();
        mappings.put(getTableName(type), table);
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
                put("include_in_all", false);
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
        makeRequest("PUT", "/" + esIndexName, doc);
    }
}
