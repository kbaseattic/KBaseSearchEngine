package kbaserelationengine.search.test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
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
import kbaserelationengine.search.ObjectData;
import kbaserelationengine.system.IndexingRules;
import kbaserelationengine.system.ObjectTypeParsingRules;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorageTest {
    public static final boolean cleanup = true;
    
    private static ElasticIndexingStorage indexStorage;
    private static File tempDir = null;
    
    @BeforeClass
    public static void prepare() throws Exception {
        tempDir = new File("test_local/temp_files/esbulk");
        String indexNamePrefix = "test_" + System.currentTimeMillis() + ".";
        indexStorage = new ElasticIndexingStorage(
                new HttpHost("localhost", 9200), tempDir);
        indexStorage.setIndexNamePrefix(indexNamePrefix);
        cleanup();
        tempDir.mkdirs();
    }
    
    @AfterClass
    public static void teardown() throws Exception {
        if (cleanup) {
            cleanup();
        }
    }
    
    private static void cleanup() throws Exception {
        Set<String> indNames = indexStorage.listIndeces();
        for (String index : indNames) {
            if (!index.startsWith("test_")) {
                System.out.println("Skipping Elasticsearch index: " + index);
                continue;
            }
            System.out.println("Deleting Elasticsearch index: " + index);
            indexStorage.deleteIndex(index);
        }
        if (tempDir != null && tempDir.exists()) {
            FileUtils.deleteQuietly(tempDir);
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
        String parentJson = null;
        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource("genome01")) {
            parentJson = ObjectParser.extractParentFragment(parsingRules, jts);
        }
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
                    "MyGenome.1", System.currentTimeMillis(), parentJson, Collections.emptyMap(),
                    false, parsingRules.getIndexingRules());
        }
        //indexStorage.refreshIndex(indexStorage.getIndex(parsingRules.getGlobalObjectType()));
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
        List<ObjectData> objList = indexStorage.getObjectsByIds(
                new HashSet<>(Arrays.asList(id)));
        Assert.assertEquals(1, objList.size());
        Map<String, Object> obj = (Map<String, Object>)objList.get(0).data;
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
        GUID id11 = new GUID("WS:2/1/1");
        indexStorage.indexObject(id11, objType, "{\"prop1\":\"abc 123\"}", "obj.1", 0, null, null,
                false, indexingRules);
        GUID id2 = new GUID("WS:2/2/1");
        indexStorage.indexObject(id2, objType, "{\"prop1\":\"abd\"}", "obj.2", 0, null, null,
                false, indexingRules);
        GUID id3 = new GUID("WS:3/1/1");
        indexStorage.indexObject(id3, objType, "{\"prop1\":\"abc\"}", "obj.3", 0, null, null,
                false, indexingRules);
        GUID id12 = new GUID("WS:2/1/2");
        indexStorage.indexObject(id12, objType, "{\"prop1\":\"abc 124\"}", "obj.1", 0, null, null,
                false, indexingRules);
        GUID id13 = new GUID("WS:2/1/3");
        indexStorage.indexObject(id13, objType, "{\"prop1\":\"abc 125\"}", "obj.1", 0, null, null,
                false, indexingRules);
        //indexStorage.refreshIndex(indexStorage.getIndex(objType));
        checkIdInSet(indexStorage.searchIdsByText(objType, "abc", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false), 1, id13);
        checkIdInSet(indexStorage.searchIdsByText(objType, "125", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false), 1, id13);
        Assert.assertEquals(0, indexStorage.searchIdsByText(objType, "123", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false).size());
        checkIdInSet(indexStorage.searchIdsByText(objType, "abd", null, 
                new LinkedHashSet<>(Arrays.asList(2)), false), 1, id2);
        checkIdInSet(indexStorage.searchIdsByText(objType, "abc", null, 
                new LinkedHashSet<>(Arrays.asList(3)), false), 1, id3);
    }
    
    @Test
    public void testSharing() throws Exception {
        String objType = "Sharable";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop2"));
        ir.setKeywordType("integer");
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        GUID id1 = new GUID("WS:10/1/1");
        indexStorage.indexObject(id1, objType, "{\"prop2\": 123}", "obj.1", 0, null, null,
                false, indexingRules);
        GUID id2 = new GUID("WS:10/1/2");
        indexStorage.indexObject(id2, objType, "{\"prop2\": 124}", "obj.1", 0, null, null,
                false, indexingRules);
        GUID id3 = new GUID("WS:10/1/3");
        indexStorage.indexObject(id3, objType, "{\"prop2\": 125}", "obj.1", 0, null, null,
                false, indexingRules);
        //indexStorage.refreshIndex(indexStorage.getIndex(objType));
        Assert.assertEquals(0, indexStorage.lookupIdsByKey(objType, "prop2", 123, 
                new LinkedHashSet<>(Arrays.asList(10)), false).size());
        checkIdInSet(indexStorage.lookupIdsByKey(objType, "prop2", 125, 
                new LinkedHashSet<>(Arrays.asList(10)), false), 1, id3);
        indexStorage.shareObject(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        checkIdInSet(indexStorage.lookupIdsByKey(objType, "prop2", 123, 
                new LinkedHashSet<>(Arrays.asList(11)), false), 1, id1);
        Assert.assertEquals(0, indexStorage.lookupIdsByKey(objType, "prop2", 124, 
                new LinkedHashSet<>(Arrays.asList(11)), false).size());
        Assert.assertEquals(0, indexStorage.lookupIdsByKey(objType, "prop2", 125, 
                new LinkedHashSet<>(Arrays.asList(11)), false).size());
        indexStorage.shareObject(new LinkedHashSet<>(Arrays.asList(id2)), 11);
        Assert.assertEquals(0, indexStorage.lookupIdsByKey(objType, "prop2", 123, 
                new LinkedHashSet<>(Arrays.asList(11)), false).size());
        checkIdInSet(indexStorage.lookupIdsByKey(objType, "prop2", 124, 
                new LinkedHashSet<>(Arrays.asList(11)), false), 1, id2);
        Assert.assertEquals(0, indexStorage.lookupIdsByKey(objType, "prop2", 125, 
                new LinkedHashSet<>(Arrays.asList(11)), false).size());
    }
    
    private static void checkIdInSet(Set<GUID> ids, int size, GUID id) {
        Assert.assertEquals(size, ids.size());
        Assert.assertTrue("Set contains: " + ids, ids.contains(id));
    }
}
