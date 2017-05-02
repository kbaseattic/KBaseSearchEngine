package kbaserelationengine.search.test;

import java.io.File;
import java.io.IOException;
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
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.MatchValue;
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
    
    private static MatchFilter ft(String fullText) {
        return MatchFilter.create().withFullTextInAll(fullText);
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
        Map<String, Integer> typeToCount = indexStorage.searchTypes(ft("Rfah"), 
                AccessFilter.create().withAdmin(true));
        Assert.assertEquals(1, typeToCount.size());
        String type = typeToCount.keySet().iterator().next();
        Assert.assertEquals(1, (int)typeToCount.get(type));
        GUID expectedGUID = new GUID("WS:1/1/1:feature/NewGenome.CDS.6210");
        // Admin mode
        Set<GUID> ids = indexStorage.searchIds(type, ft("RfaH"), null, 
                AccessFilter.create().withAdmin(true));
        Assert.assertEquals(1, ids.size());
        GUID id = ids.iterator().next();
        Assert.assertEquals(expectedGUID, id);
        // Wrong groups
        ids = indexStorage.searchIds(type, ft("RfaH"), null, 
                AccessFilter.create().withAccessGroups(2,3));
        Assert.assertEquals(0, ids.size());
        // Right groups
        Set<Integer> accessGroupIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3));
        ids = indexStorage.searchIds(type, ft("RfaH"), null, 
                AccessFilter.create().withAccessGroups(accessGroupIds));
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
        ids = indexStorage.searchIds(type, MatchFilter.create().withLookupInKey(
                "ontology_terms", "SSO:000008186"), null,
                AccessFilter.create().withAccessGroups(accessGroupIds));
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
        checkIdInSet(indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id13);
        checkIdInSet(indexStorage.searchIds(objType, ft("125"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id13);
        Assert.assertEquals(0, indexStorage.searchIds(objType, ft("123"), null, 
                AccessFilter.create().withAccessGroups(2)).size());
        checkIdInSet(indexStorage.searchIds(objType, ft("abd"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id2);
        checkIdInSet(indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(3)), 1, id3);
        // With all history
        Assert.assertEquals(1, indexStorage.searchIds(objType, ft("123"), null, 
                AccessFilter.create().withAccessGroups(2).withAllHistory(true)).size());
        Assert.assertEquals(3, indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2).withAllHistory(true)).size());
    }
    
    private Set<GUID> lookupIdsByKey(String objType, String keyName, Object value, 
            AccessFilter af) throws IOException {
        return indexStorage.searchIds(objType, MatchFilter.create().withLookupInKey(
                keyName, new MatchValue(value)), null, af);
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
        AccessFilter af10 = AccessFilter.create().withAccessGroups(10);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af10).size());
        checkIdInSet(lookupIdsByKey(objType, "prop2", 125, af10), 1, id3);
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        AccessFilter af11 = AccessFilter.create().withAccessGroups(11);
        checkIdInSet(lookupIdsByKey(objType, "prop2", 123, af11), 1, id1);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 124, af11).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af11).size());
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 11);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af11).size());
        checkIdInSet(lookupIdsByKey(objType, "prop2", 124, af11), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af11).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 11);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af11).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 124, af11).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af11).size());
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 12);
        AccessFilter af1x = AccessFilter.create().withAccessGroups(11, 12);
        checkIdInSet(lookupIdsByKey(objType, "prop2", 123, af1x), 1, id1);
        checkIdInSet(lookupIdsByKey(objType, "prop2", 124, af1x), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af1x).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af1x).size());
        checkIdInSet(lookupIdsByKey(objType, "prop2", 124, af1x), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af1x).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 12);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af1x).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 124, af1x).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 125, af1x).size());
    }
    
    @Test
    public void testPublic() throws Exception {
        String objType = "Publishable";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop3"));
        ir.setFullText(true);
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        GUID id1 = new GUID("WS:20/1/1");
        GUID id2 = new GUID("WS:20/2/1");
        indexStorage.indexObject(id1, objType, "{\"prop3\": \"private gggg\"}", "obj.1", 0, null, null,
                false, indexingRules);
        indexStorage.indexObject(id2, objType, "{\"prop3\": \"public gggg\"}", "obj.2", 0, null, null,
                true, indexingRules);
        checkIdInSet(lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        checkIdInSet(lookupIdsByKey(objType, "prop3", "public", 
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id2);
        indexStorage.publishObjects(new LinkedHashSet<>(Arrays.asList(id1)));
        checkIdInSet(lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        checkIdInSet(lookupIdsByKey(objType, "prop3", "private",
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id1);
        indexStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(id1)));
        checkIdInSet(lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        checkIdInSet(lookupIdsByKey(objType, "prop3", "public",
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id2);
        indexStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(id2)));
        checkIdInSet(lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        checkIdInSet(lookupIdsByKey(objType, "prop3", "public", 
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop3", "private", 
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop3", "public", 
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
    }
    
    private static void checkIdInSet(Set<GUID> ids, int size, GUID id) {
        Assert.assertEquals(size, ids.size());
        Assert.assertTrue("Set contains: " + ids, ids.contains(id));
    }
}
