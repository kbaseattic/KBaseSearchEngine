package kbaserelationengine.search.test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import kbaserelationengine.system.IndexingRules;
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
        GUID expectedGUID = new GUID("WS:1/1/1:feature/NewGenome.CDS.6210");
        // Admin mode
        Set<GUID> ids = indexStorage.searchIdsByText(type, "RfaH", null, null, true);
        Assert.assertEquals(1, ids.size());
        GUID id = ids.iterator().next();
        Assert.assertEquals(expectedGUID, id);
        // Wrong groups
        Set<Integer> accessGroupIds = new LinkedHashSet<>(Arrays.asList(2, 3));
        ids = indexStorage.searchIdsByText(type, "RfaH", null, accessGroupIds, false);
        Assert.assertEquals(0, ids.size());
        // Right groups
        accessGroupIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3));
        ids = indexStorage.searchIdsByText(type, "RfaH", null, accessGroupIds, false);
        Assert.assertEquals(1, ids.size());
        id = ids.iterator().next();
        Assert.assertEquals(expectedGUID, id);
        // Check object loading by IDs
        List<Object> objList = indexStorage.getObjectsByIds(
                new HashSet<>(Arrays.asList(id)));
        Assert.assertEquals(1, objList.size());
        Map<String, Object> obj = (Map<String, Object>)objList.get(0);
        Assert.assertTrue(obj.containsKey("id"));
        Assert.assertTrue(obj.containsKey("location"));
        Assert.assertTrue(obj.containsKey("function"));
        Assert.assertTrue(obj.containsKey("type"));
        // Search by keyword
        ids = indexStorage.lookupIdsByKey(type, "ontology_terms", "SSO:000008186", accessGroupIds, false);
        Assert.assertEquals(1, ids.size());
        id = ids.iterator().next();
        Assert.assertEquals(expectedGUID, id);
    }
    
    @Test
    public void testVersions() throws Exception {
        String objType = "Simple";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop1"));
        ir.setFullText(true);
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        GUID id1 = new GUID("WS:2/1/1");
        indexStorage.indexObject(id1, objType, "{\"prop1\":\"abc 123\"}", indexingRules);
        GUID id2 = new GUID("WS:2/1/2");
        indexStorage.indexObject(id2, objType, "{\"prop1\":\"abc 124\"}", indexingRules);
        GUID id3 = new GUID("WS:2/1/3");
        indexStorage.indexObject(id3, objType, "{\"prop1\":\"abc 125\"}", indexingRules);
        indexStorage.refreshIndex(indexStorage.getIndex(objType));
        Set<GUID> ids = indexStorage.searchIdsByText(objType, "abc", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false);
        Assert.assertEquals(1, ids.size());
        Assert.assertEquals(id3, ids.iterator().next());
        Assert.assertEquals(0, indexStorage.searchIdsByText(objType, "123", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false).size());
    }
}
