package kbaserelationengine.search.test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;

import junit.framework.Assert;
import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.parse.IdMapper;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.parse.SimpleIdConsumer;
import kbaserelationengine.parse.SimpleSubObjectConsumer;
import kbaserelationengine.parse.SubObjectConsumer;
import kbaserelationengine.parse.test.SubObjectExtractorTest;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.system.ObjectTypeParsingRules;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorageTest {
    public static final boolean cleanupAfter = true;
    
    private static ElasticIndexingStorage indexStorage;
    
    @BeforeClass
    public static void prepare() throws Exception {
        String indexNamePrefix = "test_" + System.currentTimeMillis() + ".";
        indexStorage = new ElasticIndexingStorage(
                new HttpHost("localhost", 9200));
        indexStorage.setIndexNamePrefix(indexNamePrefix);
    }
    
    @AfterClass
    public static void teardown() throws Exception {
        if (!cleanupAfter) {
            return;
        }
        Set<String> indNames = indexStorage.listIndeces();
        for (String index : indNames) {
            if (!index.startsWith("test_")) {
                System.out.println("Skipping Elasticsearch index: " + index);
                continue;
            }
            System.out.println("Deleting Elasticsearch index: " + index);
            indexStorage.deleteIndex(index);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSave() throws Exception {
        Map<String, Object> parsingRulesObj = UObject.getMapper().readValue(
                new File("resources/types/GenomeFeature.json"), Map.class);
        ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj);
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource("genome01")) {
            ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
        }
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                        parsingRules.getRelationPathToRules(), subJts, idConsumer);
            }
            GUID id = ObjectParser.prepareGUID(parsingRules, "1/1/1", path, idConsumer);
            indexStorage.indexObject(id, parsingRules.getGlobalObjectType(), subJson, 
                    parsingRules.getIndexingRules());
        }
        indexStorage.refreshIndex(indexStorage.getIndex(parsingRules.getGlobalObjectType()));
        Map<String, Integer> typeToCount = indexStorage.searchTypeByText("Rfah", null, false);
        Assert.assertEquals(1, typeToCount.size());
        String type = typeToCount.keySet().iterator().next();
        Assert.assertEquals(1, (int)typeToCount.get(type));
        Set<GUID> ids = indexStorage.searchIdsByText(type, "RfaH", 
                null, null, false);
        Assert.assertEquals(1, ids.size());
        GUID id = ids.iterator().next();
        Assert.assertEquals("WS:1/1/1:feature/NewGenome.CDS.6210", id.toString());
        List<Object> objList = indexStorage.getObjectsByIds(
                new HashSet<>(Arrays.asList(id)));
        Assert.assertEquals(1, objList.size());
        Map<String, Object> obj = (Map<String, Object>)objList.get(0);
        Assert.assertTrue(obj.containsKey("id"));
        Assert.assertTrue(obj.containsKey("location"));
        Assert.assertTrue(obj.containsKey("function"));
        Assert.assertTrue(obj.containsKey("type"));
    }
}
