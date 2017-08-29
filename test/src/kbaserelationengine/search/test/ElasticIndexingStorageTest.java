package kbaserelationengine.search.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import kbaserelationengine.parse.KeywordParser;
import kbaserelationengine.parse.KeywordParser.ObjectLookupProvider;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.parse.ParsedObject;
import kbaserelationengine.parse.SimpleIdConsumer;
import kbaserelationengine.parse.SimpleSubObjectConsumer;
import kbaserelationengine.parse.SubObjectConsumer;
import kbaserelationengine.parse.test.SubObjectExtractorTest;
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.MatchValue;
import kbaserelationengine.search.ObjectData;
import kbaserelationengine.search.PostProcessing;
import kbaserelationengine.system.IndexingRules;
import kbaserelationengine.system.ObjectTypeParsingRules;
import kbaserelationengine.test.common.TestCommon;
import kbaserelationengine.test.controllers.elasticsearch.ElasticSearchController;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorageTest {
    
    private static ElasticIndexingStorage indexStorage;
    private static File tempDir = null;
    private static ObjectLookupProvider objLookup;
    private static ElasticSearchController es;
    
    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();
        final Path tdir = Paths.get(TestCommon.getTempDir());
        tempDir = tdir.resolve("ElasticIndexingStorageTest").toFile();
        FileUtils.deleteQuietly(tempDir);
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tdir);
        String indexNamePrefix = "test_" + System.currentTimeMillis() + ".";
        indexStorage = new ElasticIndexingStorage(
                new HttpHost("localhost", es.getServerPort()), tempDir);
        indexStorage.setIndexNamePrefix(indexNamePrefix);
        tempDir.mkdirs();
        objLookup = new ObjectLookupProvider() {
            
            @Override
            public Set<String> resolveWorkspaceRefs(List<GUID> callerRefPath, Set<String> refs)
                    throws IOException {
                for (String ref : refs) {
                    try {
                        GUID pguid = new GUID("WS:" + ref);
                        boolean indexed = indexStorage.checkParentGuidsExist(null, new LinkedHashSet<>(
                                Arrays.asList(pguid))).get(pguid);
                        if (!indexed) {
                            indexObject("Assembly", "assembly01", pguid, "MyAssembly.1");
                            indexObject("AssemblyContig", "assembly01", pguid, "MyAssembly.1");
                            Assert.assertTrue(indexStorage.checkParentGuidsExist(null, new LinkedHashSet<>(
                                    Arrays.asList(pguid))).get(pguid));
                        }
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                return refs;
            }
            
            @Override
            public Map<GUID, ObjectData> lookupObjectsByGuid(Set<GUID> guids)
                    throws IOException {
                List<ObjectData> objList = indexStorage.getObjectsByIds(guids);
                return objList.stream().collect(Collectors.toMap(od -> od.guid, Function.identity()));
            }
            
            @Override
            public ObjectTypeParsingRules getTypeDescriptor(String type) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parsingRulesObj = UObject.getMapper().readValue(
                            new File("resources/types/" + type + ".json"), Map.class);
                    return ObjectTypeParsingRules.fromObject(parsingRulesObj);
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            
            @Override
            public Map<GUID, String> getTypesForGuids(Set<GUID> guids) throws IOException {
                PostProcessing pp = new PostProcessing();
                pp.objectData = false;
                pp.objectKeys = false;
                pp.objectInfo = true;
                Map<GUID, String> ret = indexStorage.getObjectsByIds(guids, pp).stream().collect(
                        Collectors.toMap(od -> od.guid, od -> od.type));
                return ret;
            }
        };
    }
    
    @AfterClass
    public static void teardown() throws Exception {
        if (es != null) {
            es.destroy(TestCommon.getDeleteTempFiles());
        }
        if (tempDir != null && tempDir.exists() && TestCommon.getDeleteTempFiles()) {
            FileUtils.deleteQuietly(tempDir);
        }
    }
    
    private static MatchFilter ft(String fullText) {
        return MatchFilter.create().withFullTextInAll(fullText);
    }
    
    private static void indexObject(GUID id, String objectType, String json, String objectName,
            long timestamp, String parentJsonValue, boolean isPublic,
            List<IndexingRules> indexingRules) throws IOException, ObjectParseException {
        ParsedObject obj = KeywordParser.extractKeywords(objectType, json, parentJsonValue, 
                indexingRules, objLookup, null);
        indexStorage.indexObject(id, objectType, obj, objectName, timestamp, parentJsonValue, 
                isPublic, indexingRules);
    }
    
    @SuppressWarnings("unchecked")
    private static void indexObject(String type, String jsonResource, GUID ref, String objName)
            throws Exception {
        Map<String, Object> parsingRulesObj = UObject.getMapper().readValue(
                new File("resources/types/" + type + ".json"), Map.class);
        ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj);
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        String parentJson = null;
        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            parentJson = ObjectParser.extractParentFragment(parsingRules, jts);
        }
        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
        }
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getPrimaryKeyPath() != null || parsingRules.getRelationRules() != null) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                            parsingRules.getRelationRules(), subJts, idConsumer);
                }
            }
            GUID id = ObjectParser.prepareGUID(parsingRules, ref, path, idConsumer);
            indexObject(id, parsingRules.getGlobalObjectType(), subJson, 
                    objName, System.currentTimeMillis(), parentJson,
                    false, parsingRules.getIndexingRules());
        }

    }
    
    private static ObjectData getIndexedObject(GUID guid) throws Exception {
        return indexStorage.getObjectsByIds(new LinkedHashSet<>(Arrays.asList(guid))).get(0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testFeatures() throws Exception {
        indexObject("GenomeFeature", "genome01", new GUID("WS:1/1/1"), "MyGenome.1");
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
        ObjectData featureIndex = objList.get(0);
        //System.out.println("GenomeFeature index: " + featureIndex);
        Map<String, Object> obj = (Map<String, Object>)featureIndex.data;
        Assert.assertTrue(obj.containsKey("id"));
        Assert.assertTrue(obj.containsKey("location"));
        Assert.assertTrue(obj.containsKey("function"));
        Assert.assertTrue(obj.containsKey("type"));
        Assert.assertEquals("NC_000913", featureIndex.keyProps.get("contig_id"));
        String contigGuidText = featureIndex.keyProps.get("contig_guid");
        Assert.assertNotNull(contigGuidText);
        ObjectData contigIndex = getIndexedObject(new GUID(contigGuidText));
        //System.out.println("AssemblyContig index: " + contigIndex);
        Assert.assertEquals("NC_000913", "" + contigIndex.keyProps.get("contig_id"));
        // Search by keyword
        ids = indexStorage.searchIds(type, MatchFilter.create().withLookupInKey(
                "ontology_terms", "SSO:000008186"), null,
                AccessFilter.create().withAccessGroups(accessGroupIds));
        Assert.assertEquals(1, ids.size());
        id = ids.iterator().next();
        Assert.assertEquals(expectedGUID, id);
    }
    
    @Test
    public void testGenome() throws Exception {
        indexObject("Genome", "genome01", new GUID("WS:1/1/1"), "MyGenome.1");
        Set<GUID> guids = indexStorage.searchIds("Genome", MatchFilter.create().withLookupInKey(
                "features", new MatchValue(1, null)), null, AccessFilter.create().withAdmin(true));
        Assert.assertEquals(1, guids.size());
        ObjectData genomeIndex = indexStorage.getObjectsByIds(guids).get(0);
        //System.out.println("Genome index: " + genomeIndex);
        Assert.assertTrue(genomeIndex.keyProps.containsKey("features"));
        Assert.assertEquals("3", "" + genomeIndex.keyProps.get("features"));
        Assert.assertEquals("1", "" + genomeIndex.keyProps.get("contigs"));
        Assert.assertEquals("MyAssembly.1", genomeIndex.keyProps.get("assembly"));
        String assemblyGuidText = genomeIndex.keyProps.get("assembly_guid");
        Assert.assertNotNull(assemblyGuidText);
        ObjectData assemblyIndex = getIndexedObject(new GUID(assemblyGuidText));
        //System.out.println("Assembly index: " + genomeIndex);
        Assert.assertEquals("1", "" + assemblyIndex.keyProps.get("contigs"));
    }
    
    @Test
    public void testVersions() throws Exception {
        String objType = "Simple";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop1"));
        ir.setFullText(true);
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        GUID id11 = new GUID("WS:2/1/1");
        indexObject(id11, objType, "{\"prop1\":\"abc 123\"}", "obj.1", 0, null,
                false, indexingRules);
        checkIdInSet(indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id11);
        GUID id2 = new GUID("WS:2/2/1");
        indexObject(id2, objType, "{\"prop1\":\"abd\"}", "obj.2", 0, null,
                false, indexingRules);
        GUID id3 = new GUID("WS:3/1/1");
        indexObject(id3, objType, "{\"prop1\":\"abc\"}", "obj.3", 0, null,
                false, indexingRules);
        checkIdInSet(indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id11);
        GUID id12 = new GUID("WS:2/1/2");
        indexObject(id12, objType, "{\"prop1\":\"abc 124\"}", "obj.1", 0, null,
                false, indexingRules);
        checkIdInSet(indexStorage.searchIds(objType, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id12);
        GUID id13 = new GUID("WS:2/1/3");
        indexObject(id13, objType, "{\"prop1\":\"abc 125\"}", "obj.1", 0, null,
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
        Set<GUID> ret = indexStorage.searchIds(objType, MatchFilter.create().withLookupInKey(
                keyName, new MatchValue(value)), null, af);
        PostProcessing pp = new PostProcessing();
        pp.objectInfo = true;
        pp.objectData = true;
        pp.objectKeys = true;
        indexStorage.getObjectsByIds(ret, pp);
        return ret;
    }
    
    @Test
    public void testSharing() throws Exception {
        String objType = "Sharable";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop2"));
        ir.setKeywordType("integer");
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        GUID id1 = new GUID("WS:10/1/1");
        indexObject(id1, objType, "{\"prop2\": 123}", "obj.1", 0, null,
                false, indexingRules);
        GUID id2 = new GUID("WS:10/1/2");
        indexObject(id2, objType, "{\"prop2\": 124}", "obj.1", 0, null,
                false, indexingRules);
        GUID id3 = new GUID("WS:10/1/3");
        indexObject(id3, objType, "{\"prop2\": 125}", "obj.1", 0, null,
                false, indexingRules);
        AccessFilter af10 = AccessFilter.create().withAccessGroups(10);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 123, af10).size());
        checkIdInSet(lookupIdsByKey(objType, "prop2", 125, af10), 1, id3);
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        AccessFilter af11 = AccessFilter.create().withAccessGroups(11);
        checkIdInSet(lookupIdsByKey(objType, "prop2", 123, af11), 1, id1);
        checkIdInSet(lookupIdsByKey(objType, "prop2", 125, af10), 1, id3);
        Assert.assertEquals(0, lookupIdsByKey(objType, "prop2", 124, af11).size());
        checkIdInSet(lookupIdsByKey(objType, "prop2", 124, 
                AccessFilter.create().withAccessGroups(10).withAllHistory(true)), 1, id2);       
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
        indexObject(id1, objType, "{\"prop3\": \"private gggg\"}", "obj.1", 0, null,
                false, indexingRules);
        indexObject(id2, objType, "{\"prop3\": \"public gggg\"}", "obj.2", 0, null,
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
        Assert.assertEquals("Set contains: " + ids, size, ids.size());
        Assert.assertTrue("Set contains: " + ids, ids.contains(id));
    }
}
