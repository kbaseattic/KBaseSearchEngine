package kbasesearchengine.test.search;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableMap;

import junit.framework.Assert;
import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.IdMapper;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.parse.SimpleIdConsumer;
import kbasesearchengine.parse.SimpleSubObjectConsumer;
import kbasesearchengine.parse.SubObjectConsumer;
import kbasesearchengine.parse.KeywordParser.ObjectLookupProvider;
import kbasesearchengine.search.AccessFilter;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.MatchFilter;
import kbasesearchengine.search.MatchValue;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.test.controllers.elasticsearch.ElasticSearchController;
import kbasesearchengine.test.parse.SubObjectExtractorTest;
import org.junit.rules.ExpectedException;
import us.kbase.common.service.UObject;


public class ElasticIndexingStorageTest {

    private static ElasticIndexingStorage indexStorage;
    private static File tempDir = null;
    private static ObjectLookupProvider objLookup;
    private static ElasticSearchController es;

    //TODO TEST should delete indexes between every test to keep them independent

    public ElasticIndexingStorageTest() {

    }

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
            public Set<GUID> resolveRefs(List<GUID> callerRefPath, Set<GUID> refs) {
                for (GUID pguid : refs) {
                    try {
                        boolean indexed = indexStorage.checkParentGuidsExist(new LinkedHashSet<>(
                                Arrays.asList(pguid))).get(pguid);
                        if (!indexed) {
                            indexObject("Assembly", "assembly01", pguid, "MyAssembly.1");
                            indexObject("AssemblyContig", "assembly01", pguid, "MyAssembly.1");
                            Assert.assertTrue(indexStorage.checkParentGuidsExist(new LinkedHashSet<>(
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
                    throws FatalIndexingException {
                List<ObjectData> objList;
                try {
                    objList = indexStorage.getObjectsByIds(guids);
                } catch (IOException e) {
                    throw new FatalIndexingException(e.getMessage(), e);
                }
                return objList.stream().collect(
                        Collectors.toMap(od -> od.getGUID(), Function.identity()));
            }

            @Override
            public ObjectTypeParsingRules getTypeDescriptor(SearchObjectType type) {
                try {
                    final File rulesFile = new File("resources/types/" + type.getType() + ".yaml");
                    return ObjectTypeParsingRulesFileParser.fromFile(rulesFile)
                            .get(type.getVersion() - 1);
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }

            @Override
            public Map<GUID, SearchObjectType> getTypesForGuids(Set<GUID> guids)
                    throws FatalIndexingException {
                PostProcessing pp = new PostProcessing();
                pp.objectData = false;
                pp.objectKeys = false;
                pp.objectInfo = true;
                try {
                    return indexStorage.getObjectsByIds(guids, pp).stream().collect(
                            Collectors.toMap(od -> od.getGUID(), od -> od.getType().get()));
                } catch (IOException e) {
                    throw new FatalIndexingException(e.getMessage(), e);
                }
            }
        };
    }

    @After
    public void cleanup() throws Exception {
        indexStorage.dropData();
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

    private static void indexObject(
            final GUID id,
            final ObjectTypeParsingRules rule,
            final String json,
            final String objectName,
            final Instant timestamp,
            final String parentJsonValue,
            final boolean isPublic)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {
        ParsedObject obj = KeywordParser.extractKeywords(rule.getGlobalObjectType(), json,
                parentJsonValue, rule.getIndexingRules(), objLookup, null);
        final SourceData data = SourceData.getBuilder(new UObject(json), objectName, "creator")
                .build();
        indexStorage.indexObject(rule, data, timestamp, parentJsonValue, id, obj, isPublic);
    }
    
    private static void indexObject(
            final String type,
            final String jsonResource,
            final GUID ref,
            final String objName)
            throws Exception {
        final File file = new File("resources/types/" + type + ".yaml");
        ObjectTypeParsingRules parsingRules = ObjectTypeParsingRulesFileParser.fromFile(file).get(0);
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
            if (parsingRules.getSubObjectIDPath().isPresent()) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getSubObjectIDPath().get(), subJts, idConsumer);
                }
            }
            GUID id = ObjectParser.prepareGUID(parsingRules, ref, path, idConsumer);
            indexObject(id, parsingRules, subJson, objName, Instant.now(), parentJson, false);
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
        List<String> type = ImmutableList.of(typeToCount.keySet().iterator().next());
        Assert.assertEquals(1, (int)typeToCount.get(type.get(0)));
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
        Map<String, Object> obj = (Map<String, Object>)featureIndex.getData().get();
        Assert.assertTrue(obj.containsKey("id"));
        Assert.assertTrue(obj.containsKey("location"));
        Assert.assertTrue(obj.containsKey("function"));
        Assert.assertTrue(obj.containsKey("type"));
        Assert.assertEquals("NC_000913", featureIndex.getKeyProperties().get("contig_id"));
        String contigGuidText = featureIndex.getKeyProperties().get("contig_guid");
        Assert.assertNotNull("missing contig_guid", contigGuidText);
        ObjectData contigIndex = getIndexedObject(new GUID(contigGuidText));
        //System.out.println("AssemblyContig index: " + contigIndex);
        Assert.assertEquals("NC_000913", "" + contigIndex.getKeyProperties().get("contig_id"));
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
        System.out.println("*** start testGenome***");
        indexObject("Genome", "genome01", new GUID("WS:1/1/1"), "MyGenome.1");
        Set<GUID> guids = indexStorage.searchIds(ImmutableList.of("Genome"),
                MatchFilter.create().withLookupInKey(
                        "features", new MatchValue(1, null)),
                null, AccessFilter.create().withAdmin(true));
        Assert.assertEquals(1, guids.size());
        ObjectData genomeIndex = indexStorage.getObjectsByIds(guids).get(0);
        //System.out.println("Genome index: " + genomeIndex);
        Assert.assertTrue(genomeIndex.getKeyProperties().containsKey("features"));
        Assert.assertEquals("3", "" + genomeIndex.getKeyProperties().get("features"));
        Assert.assertEquals("1", "" + genomeIndex.getKeyProperties().get("contigs"));
        String assemblyGuidText = genomeIndex.getKeyProperties().get("assembly_guid");
        Assert.assertNotNull(assemblyGuidText);
        ObjectData assemblyIndex = getIndexedObject(new GUID(assemblyGuidText));
        //System.out.println("Assembly index: " + genomeIndex);
        Assert.assertEquals("1", "" + assemblyIndex.getKeyProperties().get("contigs"));
        System.out.println("*** end testGenome***");
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testMultiTypeSearchValidation1() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid list of object types. List is null.");

        // null list of types
        indexStorage.searchIds(null,
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));
    }

    @Test
    public void testMultiTypeSearchValidation2() throws Exception {
        indexObject("Genome", "genome01", new GUID("WS:1/1/1"), "MyGenome.1");

        // empty list
        Set<GUID> guids = indexStorage.searchIds(new ArrayList<String>(),
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));

        // genome object + 2 parent guids (see prepare)
        assertThat("incorrect number of results", guids.size(), is(3));
    }

    @Test
    public void testMultiTypeSearchValidation3() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Invalid list of object types. Contains one or more null elements.");

        // list containing a mix of null and non-null types
        List<String> objectTypes = new ArrayList<>();
        objectTypes.add(null);
        objectTypes.add("Narrative");
        indexStorage.searchIds(objectTypes,
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));
    }

    @Test
    public void testMultiTypeSearchValidation4() throws Exception {
        indexObject("Genome", "genome01", new GUID("WS:1/1/1"), "MyGenome.1");
        List<String> objectTypes;
        Set<GUID> guids;

        // list with single non-null element
        objectTypes = new ArrayList<>();
        objectTypes.add("Genome");
        guids = indexStorage.searchIds(objectTypes,
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));

        assertThat("incorrect number of results", guids.size(), is(1));
    }

    @Test
    public void testMultiTypeSearchValidation5() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Invalid list of object types. List size exceeds maximum limit of 50");

        // list exceeding max size
        List<String> objectTypes = new ArrayList<>();
        for(int ii=0; ii< ElasticIndexingStorage.MAX_OBJECT_TYPES_SIZE+1; ii++) {
            objectTypes.add("Narrative");
        }

        indexStorage.searchIds(objectTypes,
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));
    }

    @Test
    public void testMultiTypeSearch() throws Exception {
        // search for Genome objects
        indexObject("Genome", "genome01", new GUID("WS:1/3/1"), "MyGenome.1");

        Set<GUID> guids;

        guids = indexStorage.searchIds(ImmutableList.of("Genome"),
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));

        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "1/3/1")),
                                                                                            is(true));
        assertThat("incorrect number of objects in results", guids.size(), is(1));

        // search for Genome and Assembly objects (assembly object from prepare method)
        guids = indexStorage.searchIds(ImmutableList.of("Genome", "Assembly"),
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));

        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "1/3/1")),
                is(true));

        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "1/2/1")),
                is(true));
        assertThat("incorrect number of objects in results", guids.size(), is(2));


        // search for Genome, Assembly and AssemblyContig objects (assembly and contig objects from prepare method)
        guids = indexStorage.searchIds(ImmutableList.of("Genome", "Assembly", "AssemblyContig"),
                MatchFilter.create().withAccessGroupId(1),
                null, AccessFilter.create().withAdmin(true));

        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "1/3/1")),
                is(true));

        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "1/2/1")),
                is(true));
        assertThat("result missing expected object", guids.contains(GUID.fromRef("WS", "WS:1/2/1:contig/NC_000913")),
                is(true));
        assertThat("incorrect number of objects in results", guids.size(), is(3));
    }

    @Test
    public void testVersions() throws Exception {
        SearchObjectType objType = new SearchObjectType("Simple", 1);
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("prop1"))
                .withFullText().build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id11 = new GUID("WS:2/1/1");
        indexObject(id11, rule, "{\"prop1\":\"abc 123\"}", "obj.1", Instant.now(), null, false);
        checkIdInSet(indexStorage.searchIds(type, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id11);
        GUID id2 = new GUID("WS:2/2/1");
        indexObject(id2, rule, "{\"prop1\":\"abd\"}", "obj.2", Instant.now(), null, false);
        GUID id3 = new GUID("WS:3/1/1");
        indexObject(id3, rule, "{\"prop1\":\"abc\"}", "obj.3", Instant.now(), null, false);
        checkIdInSet(indexStorage.searchIds(type, ft("abc"), null, 
                AccessFilter.create().withAccessGroups(2)), 1, id11);
        GUID id12 = new GUID("WS:2/1/2");
        indexObject(id12, rule, "{\"prop1\":\"abc 124\"}", "obj.1", Instant.now(), null, false);
        checkIdInSet(indexStorage.searchIds(type, ft("abc"), null,
                AccessFilter.create().withAccessGroups(2)), 1, id12);
        GUID id13 = new GUID("WS:2/1/3");
        indexObject(id13, rule, "{\"prop1\":\"abc 125\"}", "obj.1", Instant.now(), null, false);
        //indexStorage.refreshIndex(indexStorage.getIndex(objType));
        checkIdInSet(indexStorage.searchIds(type, ft("abc"), null,
                AccessFilter.create().withAccessGroups(2)), 1, id13);
        checkIdInSet(indexStorage.searchIds(type, ft("125"), null,
                AccessFilter.create().withAccessGroups(2)), 1, id13);
        Assert.assertEquals(0, indexStorage.searchIds(type, ft("123"), null,
                AccessFilter.create().withAccessGroups(2)).size());
        checkIdInSet(indexStorage.searchIds(type, ft("abd"), null,
                AccessFilter.create().withAccessGroups(2)), 1, id2);
        checkIdInSet(indexStorage.searchIds(type, ft("abc"), null,
                AccessFilter.create().withAccessGroups(3)), 1, id3);
        // With all history
        Assert.assertEquals(1, indexStorage.searchIds(type, ft("123"), null,
                AccessFilter.create().withAccessGroups(2).withAllHistory(true)).size());
        Assert.assertEquals(3, indexStorage.searchIds(type, ft("abc"), null,
                AccessFilter.create().withAccessGroups(2).withAllHistory(true)).size());
    }

    private Set<GUID> lookupIdsByKey(List<String> objTypes, String keyName, Object value,
            AccessFilter af) throws IOException {
        Set<GUID> ret = indexStorage.searchIds(objTypes, MatchFilter.create().withLookupInKey(
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
        SearchObjectType objType = new SearchObjectType("Sharable",1 );
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("prop2"))
                .withKeywordType("integer").build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id1 = new GUID("WS:10/1/1");
        indexObject(id1, rule, "{\"prop2\": 123}", "obj.1", Instant.now(), null, false);
        GUID id2 = new GUID("WS:10/1/2");
        indexObject(id2, rule, "{\"prop2\": 124}", "obj.1", Instant.now(), null, false);
        GUID id3 = new GUID("WS:10/1/3");
        indexObject(id3, rule, "{\"prop2\": 125}", "obj.1", Instant.now(), null, false);
        AccessFilter af10 = AccessFilter.create().withAccessGroups(10);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 123, af10).size());
        checkIdInSet(lookupIdsByKey(type, "prop2", 125, af10), 1, id3);
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11, false);
        AccessFilter af11 = AccessFilter.create().withAccessGroups(11);
        checkIdInSet(lookupIdsByKey(type, "prop2", 123, af11), 1, id1);
        checkIdInSet(lookupIdsByKey(type, "prop2", 125, af10), 1, id3);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 124, af11).size());
        checkIdInSet(lookupIdsByKey(type, "prop2", 124,
                AccessFilter.create().withAccessGroups(10).withAllHistory(true)), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af11).size());
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 11, false);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 123, af11).size());
        checkIdInSet(lookupIdsByKey(type, "prop2", 124, af11), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af11).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 11);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 123, af11).size());
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 124, af11).size());
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af11).size());
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11, false);
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 12, false);
        AccessFilter af1x = AccessFilter.create().withAccessGroups(11, 12);
        checkIdInSet(lookupIdsByKey(type, "prop2", 123, af1x), 1, id1);
        checkIdInSet(lookupIdsByKey(type, "prop2", 124, af1x), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af1x).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 11);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 123, af1x).size());
        checkIdInSet(lookupIdsByKey(type, "prop2", 124, af1x), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af1x).size());
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id2)), 12);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 123, af1x).size());
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 124, af1x).size());
        Assert.assertEquals(0, lookupIdsByKey(type, "prop2", 125, af1x).size());
    }

    @Test
    public void testPublic() throws Exception {
        SearchObjectType objType = new SearchObjectType("Publishable", 1);
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("prop3"))
                .withFullText().build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id1 = new GUID("WS:20/1/1");
        GUID id2 = new GUID("WS:20/2/1");
        indexObject(id1, rule, "{\"prop3\": \"private gggg\"}", "obj.1", Instant.now(), null,
                false);
        indexObject(id2, rule, "{\"prop3\": \"public gggg\"}", "obj.2", Instant.now(), null, true);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withPublic(true)).size());
        checkIdInSet(lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        checkIdInSet(lookupIdsByKey(type, "prop3", "public",
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id2);
        indexStorage.publishObjects(new LinkedHashSet<>(Arrays.asList(id1)));
        checkIdInSet(lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        checkIdInSet(lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id1);
        indexStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(id1)));
        checkIdInSet(lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        checkIdInSet(lookupIdsByKey(type, "prop3", "public",
                AccessFilter.create().withAccessGroups(21).withPublic(true)), 1, id2);
        indexStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(id2)));
        checkIdInSet(lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id1);
        checkIdInSet(lookupIdsByKey(type, "prop3", "public",
                AccessFilter.create().withAccessGroups(20).withPublic(true)), 1, id2);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop3", "private",
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
        Assert.assertEquals(0, lookupIdsByKey(type, "prop3", "public",
                AccessFilter.create().withAccessGroups(21).withPublic(true)).size());
    }

    private static Set<GUID> asSet(GUID... guids) {
        return new LinkedHashSet<>(Arrays.asList(guids));
    }

    private static void checkIdInSet(Set<GUID> ids, int size, GUID id) {
        Assert.assertEquals("Set contains: " + ids, size, ids.size());
        Assert.assertTrue("Set contains: " + ids, ids.contains(id));
    }

    @Test
    public void testPublicDataPalettes() throws Exception {
        SearchObjectType objType = new SearchObjectType("ShareAndPublic", 1);
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("prop4"))
                .withKeywordType("integer").build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id1 = new GUID("WS:30/1/1");
        indexObject(id1, rule, "{\"prop4\": 123}", "obj.1", Instant.now(), null, false);
        AccessFilter af30 = AccessFilter.create().withAccessGroups(30);
        checkIdInSet(lookupIdsByKey(type, "prop4", 123, af30), 1, id1);
        AccessFilter afPub = AccessFilter.create().withPublic(true);
        Assert.assertEquals(0, lookupIdsByKey(type, "prop4", 123, afPub).size());
        // Let's share object id1 with PUBLIC workspace 31
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 31, true);
        // Should be publicly visible
        checkIdInSet(lookupIdsByKey(type, "prop4", 123, afPub), 1, id1);
        // Let's check that unshare (with no call to unpublishObjectsExternally is enough
        indexStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 31);
        // Should NOT be publicly visible
        Assert.assertEquals(0, lookupIdsByKey(type, "prop4", 123, afPub).size());
        // Let's share object id1 with NOT public workspace 31
        indexStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(id1)), 31, false);
        // Should NOT be publicly visible
        Assert.assertEquals(0, lookupIdsByKey(type, "prop4", 123, afPub).size());
        // Now let's declare workspace 31 PUBLIC
        indexStorage.publishObjectsExternally(asSet(id1), 31);
        // Should be publicly visible
        checkIdInSet(lookupIdsByKey(type, "prop4", 123, afPub), 1, id1);
        // Now let's declare workspace 31 NOT public
        indexStorage.unpublishObjectsExternally(asSet(id1), 31);
        // Should NOT be publicly visible
        Assert.assertEquals(0, lookupIdsByKey(type, "prop4", 123, afPub).size());
    }

    @Test
    public void testDeleteUndelete() throws Exception {
        SearchObjectType objType = new SearchObjectType("DelUndel", 1);
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("myprop"))
                .withFullText().build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id1 = new GUID("WS:100/2/1");
        GUID id2 = new GUID("WS:100/2/2");
        indexObject(id1, rule, "{\"myprop\": \"some stuff\"}", "myobj", Instant.now(), null,
                false);
        indexObject(id2, rule, "{\"myprop\": \"some other stuff\"}", "myobj", Instant.now(), null,
                false);
        
        final AccessFilter filter = AccessFilter.create().withAccessGroups(100);
        final AccessFilter filterAllVers = AccessFilter.create().withAccessGroups(100)
                .withAllHistory(true);

        // check ids show up before delete
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set(id2)));
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterAllVers), is(set(id1, id2)));

        // check ids show up correctly after delete
        indexStorage.deleteAllVersions(id1);
        indexStorage.refreshIndexByType(rule);
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set()));
        //TODO NOW these should probaby not show up
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterAllVers), is(set(id1, id2)));

        // check ids restored after undelete
        indexStorage.undeleteAllVersions(id1);
        indexStorage.refreshIndexByType(rule);
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set(id2)));
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterAllVers), is(set(id1, id2)));

        /* This doesn't actually test that the access group id is removed from the access
         * doc AFAIK, but I don't think that matters.
         */
    }

    @Test
    public void testPublishAllVersions() throws Exception {
        // tests the all versions method for setting objects public / non-public.
        SearchObjectType objType = new SearchObjectType("PublishAllVersions", 1);
        List<String> type = ImmutableList.of(objType.getType());
        IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("myprop"))
                .withFullText().build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        GUID id1 = new GUID("WS:200/2/1");
        GUID id2 = new GUID("WS:200/2/2");
        indexObject(id1, rule, "{\"myprop\": \"some stuff\"}", "myobj", Instant.now(), null,
                false);
        indexObject(id2, rule, "{\"myprop\": \"some other stuff\"}", "myobj", Instant.now(),
                null, false);
        
        final AccessFilter filter = AccessFilter.create()
                .withAllHistory(true).withPublic(false);
        final AccessFilter filterPublic = AccessFilter.create()
                .withAllHistory(true).withPublic(true);

        // check ids show up before publish
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set()));
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterPublic), is(set()));

        // check ids show up correctly after publish
        indexStorage.publishAllVersions(id1);
        indexStorage.refreshIndexByType(rule);
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set()));
        //TODO NOW these should probaby not show up
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterPublic), is(set(id1, id2)));

        // check ids hidden after unpublish
        indexStorage.unpublishAllVersions(id1);
        indexStorage.refreshIndexByType(rule);
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filter), is(set()));
        assertThat("incorrect ids returned", lookupIdsByKey(type, "myprop", "some",
                filterPublic), is(set()));
    }
    
    @Test
    public void testTypeVersions() throws Exception {
        /* test that types with incompatible fields but different versions index successfully. */
        final SearchObjectType type1 = new SearchObjectType("TypeVers", 5);
        // changing 10 -> 5 makes the test fail due to elasticsearch exception
        final SearchObjectType type2 = new SearchObjectType("TypeVers", 10);
        final IndexingRules idxRules1 = IndexingRules.fromPath(new ObjectJsonPath("bar"))
                .withKeywordType("integer").build();
        final IndexingRules idxRules2 = IndexingRules.fromPath(new ObjectJsonPath("bar"))
                .withKeywordType("keyword").build();
        final Instant now = Instant.now();
        final ObjectTypeParsingRules rule1 = ObjectTypeParsingRules.getBuilder(
                type1, new StorageObjectType("foo", "bar"))
                .withIndexingRule(idxRules1).build();
        final ObjectTypeParsingRules rule2 = ObjectTypeParsingRules.getBuilder(
                type2, new StorageObjectType("foo", "bar"))
                .withIndexingRule(idxRules2).build();
        
        indexObject(new GUID("WS:1/2/3"), rule1, "{\"bar\": 1}", "o1", now,
                null, false);
        indexObject(new GUID("WS:4/5/6"), rule2, "{\"bar\": \"whee\"}", "o2", now,
                null, false);
        
        final ObjectData indexedObj1 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/2/3"))).get(0);
        
        final ObjectData expected1 = ObjectData.getBuilder(new GUID("WS:1/2/3"))
                .withNullableObjectName("o1")
                .withNullableType(type1)
                .withNullableCreator("creator")
                .withNullableTimestamp(indexedObj1.getTimestamp().get())
                .withNullableData(ImmutableMap.of("bar", 1))
                .withKeyProperty("bar", "1")
                .build();
        
        //due to potential truncation of timestamp on mac
        TestCommon.assertCloseMS(indexedObj1.getTimestamp().get(), now, 0, 10);

        assertThat("incorrect indexed object", indexedObj1, is(expected1));
        
        final ObjectData indexedObj2 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:4/5/6"))).get(0);

        final ObjectData expected2 = ObjectData.getBuilder(new GUID("WS:4/5/6"))
                .withNullableObjectName("o2")
                .withNullableType(type2)
                .withNullableCreator("creator")
                .withNullableTimestamp(indexedObj2.getTimestamp().get())
                .withNullableData(ImmutableMap.of("bar", "whee"))
                .withKeyProperty("bar", "whee")
                .build();

        TestCommon.assertCloseMS(indexedObj2.getTimestamp().get(), now, 0, 10);

        assertThat("incorrect indexed object", indexedObj2, is(expected2));
        
    }
    
    @Test
    public void noIndexingRules() throws Exception {
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("NoIndexingRules", 1),
                        new StorageObjectType("foo", "bar"))
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname", "creator")
                        .withNullableCommitHash("commit")
                        .withNullableCopier("cop")
                        .withNullableMD5("emmdeefive")
                        .withNullableMethod("meth")
                        .withNullableModule("mod")
                        .withNullableVersion("ver")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1000/1/1"),
                Collections.emptyMap(),
                false);
        
        final ObjectData indexedObj =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1000/1/1"))).get(0);
        
        final ObjectData expected = ObjectData.getBuilder(new GUID("WS:1000/1/1"))
                .withNullableObjectName("objname")
                .withNullableType(new SearchObjectType("NoIndexingRules", 1))
                .withNullableCreator("creator")
                .withNullableCommitHash("commit")
                .withNullableCopier("cop")
                .withNullableMD5("emmdeefive")
                .withNullableMethod("meth")
                .withNullableModule("mod")
                .withNullableModuleVersion("ver")
                .withNullableTimestamp(Instant.ofEpochMilli(10000))
                .build();
        
        assertThat("incorrect indexed object", indexedObj, is(expected));
    }
    
    @Test
    public void excludeSubObjects() throws Exception {
        // regular object
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("ExcludeSubObjectsNorm", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname", "creator").build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:2000/1/1"),
                ImmutableMap.of(new GUID("WS:2000/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony")))),
                false);
        
        // sub object
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("ExcludeSubObjectsSub", 1),
                        new StorageObjectType("foo", "bar"))
                        .toSubObjectRule(
                                "sub", new ObjectJsonPath("subpath"), new ObjectJsonPath("id"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname", "creator").build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:2000/2/1"),
                ImmutableMap.of(new GUID("WS:2000/2/1:sub/yay"), new ParsedObject(
                        "{\"whee\": \"imaprettypony\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony")))),
                false);
        
        final Set<GUID> res = indexStorage.searchIds(
                Collections.emptyList(),
                MatchFilter.create().withFullTextInAll("imaprettypony"),
                null,
                AccessFilter.create().withAccessGroups(2000));
        assertThat("incorrect objects found", res,
                is(set(new GUID("WS:2000/1/1"), new GUID("WS:2000/2/1:sub/yay"))));
        
        final Set<GUID> res2 = indexStorage.searchIds(
                Collections.emptyList(),
                MatchFilter.create().withFullTextInAll("imaprettypony")
                        .withExcludeSubObjects(true),
                null,
                AccessFilter.create().withAccessGroups(2000));
        assertThat("incorrect objects found", res2, is(set(new GUID("WS:2000/1/1"))));
        
        final Map<String, Integer> count = indexStorage.searchTypes(
                MatchFilter.create().withFullTextInAll("imaprettypony"),
                AccessFilter.create().withAccessGroups(2000));
        
        assertThat("incorrect type count", count, is(ImmutableMap.of(
                "ExcludeSubObjectsSub", 1,
                "ExcludeSubObjectsNorm", 1)));
        
        final Map<String, Integer> count2 = indexStorage.searchTypes(
                MatchFilter.create().withFullTextInAll("imaprettypony")
                        .withExcludeSubObjects(true),
                AccessFilter.create().withAccessGroups(2000));
        
        assertThat("incorrect type count", count2, is(ImmutableMap.of(
                "ExcludeSubObjectsNorm", 1)));
    }

    private void prepareTestMultiwordSearch(GUID guid1, GUID guid2, GUID guid3) throws Exception {
        final SearchObjectType objectType = new SearchObjectType("Simple", 1);
        final IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("prop1"))
                .withFullText().build();

        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objectType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir).build();
        
        indexObject(guid1, rule,
                "{\"prop1\":\"multiWordInSearchMethod1 multiWordInSearchMethod2\"}",
                "multiword.1", Instant.now(), null, true);
        indexObject(guid2, rule, "{\"prop1\":\"multiWordInSearchMethod2\"}",
                "multiword.2", Instant.now(), null, true);
        indexObject(guid3, rule, "{\"prop1\":\"multiWordInSearchMethod1\"}",
                "multiword.3", Instant.now(), null, true);
    }


    @Test
    public void testMultiwordSearch() throws Exception{
        GUID guid1 = new GUID("WS:11/1/2");
        GUID guid2 = new GUID("WS:11/2/2");
        GUID guid3 = new GUID("WS:11/3/2");
        prepareTestMultiwordSearch(guid1, guid2, guid3);

        List<String> emtpy = new ArrayList<>();

        final kbasesearchengine.search.MatchFilter filter = new kbasesearchengine.search.MatchFilter();
        List<kbasesearchengine.search.SortingRule> sorting = null;
        AccessFilter accessFilter = AccessFilter.create().withAdmin(true);

        filter.withFullTextInAll("multiWordInSearchMethod1 multiWordInSearchMethod2");
        FoundHits hits1 = indexStorage.searchObjects(emtpy, filter,sorting, accessFilter, null, null);

        filter.withFullTextInAll("multiWordInSearchMethod1");
        FoundHits hits2 = indexStorage.searchObjects(emtpy, filter,sorting, accessFilter, null, null);


        filter.withFullTextInAll("multiWordInSearchMethod2");
        FoundHits hits3 = indexStorage.searchObjects(emtpy, filter,sorting, accessFilter, null, null);

        assertThat("did not find object1", hits1.guids, is(set(guid1)));
        assertThat("did not find object1 and object3", hits2.guids, is(set(guid1,guid3)));
        assertThat("did not find object1 and object2", hits3.guids, is(set(guid1, guid2)));

    }

    private void prepareTestLookupInKey(GUID guid1, GUID guid2, GUID guid3) throws Exception {
        final SearchObjectType objType = new SearchObjectType("SimpleNumber", 1 );
        final IndexingRules ir1 = IndexingRules.fromPath(new ObjectJsonPath("num1"))
                .withKeywordType("integer").build();
        final IndexingRules ir2 = IndexingRules.fromPath(new ObjectJsonPath("num2"))
                .withKeywordType("integer").build();

        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                objType, new StorageObjectType("foo", "bar"))
                .withIndexingRule(ir1)
                .withIndexingRule(ir2).build();

        indexObject(guid1, rule, "{\"num1\": 123, \"num2\": 123}",
                "number.1", Instant.now(), null, false);
        indexObject(guid2, rule, "{\"num1\": 1234, \"num2\": 1234}",
                "number.2", Instant.now(), null, false);
        indexObject(guid3, rule, "{\"num1\": 1236, \"num2\": 1236}",
                "number.3", Instant.now(), null, false);
    }
    @Test
    public void testLookupInKey() throws Exception{
        GUID guid1 = new GUID("WS:12/1/2");
        GUID guid2 = new GUID("WS:12/2/2");
        GUID guid3 = new GUID("WS:12/3/2");
        prepareTestLookupInKey(guid1, guid2, guid3);
        List<String> emtpy = new ArrayList<>();


        List<kbasesearchengine.search.SortingRule> sorting = null;
        AccessFilter accessFilter = AccessFilter.create().withAdmin(true);

        //key, value pair lookup
        MatchFilter filter0 = MatchFilter.create().withLookupInKey(
                "num1", "123");
        FoundHits hits0 = indexStorage.searchObjects(emtpy, filter0,sorting, accessFilter
                , null, null);
        assertThat("did not find object1 using LookupInKey with value", hits0.guids, is(set(guid1)));


        //key, range lookup
        MatchValue range1 = new MatchValue(100, 200);
        MatchValue range2 = new MatchValue(1000, 2000);
        MatchValue range3 = new MatchValue(100, 1234);

        MatchFilter filter1 = MatchFilter.create().withLookupInKey("num1", range1);
        MatchFilter filter2 = MatchFilter.create().withLookupInKey("num2", range2);
        MatchFilter filter3 = MatchFilter.create().withLookupInKey("num1", range3);

        FoundHits hits1 = indexStorage.searchObjects(emtpy, filter1,sorting, accessFilter, null, null);
        FoundHits hits2 = indexStorage.searchObjects(emtpy, filter2,sorting, accessFilter, null, null);
        FoundHits hits3 = indexStorage.searchObjects(emtpy, filter3,sorting, accessFilter, null, null);

        assertThat("did not find object1 using LookupInKey with range", hits1.guids, is(set(guid1)));
        assertThat("did not find object2 and object3 using LookupInKey with range", hits2.guids, is(set(guid2, guid3)));
        assertThat("did not find object1 and object3 using LookupInKey with range", hits3.guids, is(set(guid1, guid2)));

        //conflicting filters should return nothing
        MatchFilter filter4 = MatchFilter.create().withLookupInKey("num1", range1);
        filter4.withLookupInKey("num2", range2);
        FoundHits hits4 = indexStorage.searchObjects(emtpy, filter4,sorting, accessFilter, null, null);

        assertThat("conflicting ranges should produce 0 results", hits4.guids.isEmpty(), is(true));


        // overlapping filters should return intersection
        MatchFilter filter5 = MatchFilter.create().withLookupInKey("num1", range3);
        filter5.withLookupInKey("num2", range2);
        FoundHits hits5 = indexStorage.searchObjects(emtpy, filter5,sorting, accessFilter
                , null, null);

        assertThat("overlapping ranges did not return intersection", hits5.guids, is(set(guid2)));
    }

}
