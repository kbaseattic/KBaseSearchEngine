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
    private boolean mergeKeywords = false;
    private boolean skipFullJson = false;

    public ElasticIndexingStorage(String esHost, String esIndexName) {
        this.esHost = esHost;
        this.esIndexName = esIndexName;
    }
    
    public boolean isMergeKeywords() {
        return mergeKeywords;
    }
    
    public void setMergeKeywords(boolean mergeKeywords) {
        this.mergeKeywords = mergeKeywords;
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
                if (mergeKeywords) {
                    key = "all";
                } else {
                    key = rules.getKeyName() != null ? rules.getKeyName():
                        rules.getPath().getPathItems()[0];
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
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.putAll(indexPart);
        doc.put("_guid", (Object)id);
        doc.put("_otype", objectType);
        if (!skipFullJson) {
            doc.put("_ojson", valueJson);
        }
        int newId = 1;  // TODO: lookup for auto-incremented ID
        makeRequest("PUT", "/" + esIndexName + "/" + objectType + "/" + newId, doc);
    }
    
    @Override
    public List<Object> getObjectsByIds(Set<GUID> id) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Set<GUID> searchIdsByText(String text, List<SortingRule> sorting)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<GUID> lookupIdsByKey(String keyName, Object keyValue)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) 
            throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost(esHost)).build();
        return restClient.performRequest(reqType, urlPath, Collections.<String, String>emptyMap(),
                new StringEntity(UObject.transformObjectToString(doc)));
    }
    
    private void checkSchema(String type, List<IndexingRules> indexingRules) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        Map<String, Object> mappings = new LinkedHashMap<>();
        doc.put("mappings", mappings);
        Map<String, Object> table = new LinkedHashMap<>();
        mappings.put(type, table);
        Map<String, Object> props = new LinkedHashMap<>();
        table.put("properties", props);
        for (IndexingRules rules : indexingRules) {
            // TODO: add rules
        }
        makeRequest("PUT", "/" + esIndexName, doc);
    }
}
