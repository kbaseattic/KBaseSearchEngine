package kbasesearchengine.test.parse;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.both;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.IdMapper;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.SimpleIdConsumer;
import kbasesearchengine.parse.SimpleSubObjectConsumer;
import kbasesearchengine.parse.SubObjectConsumer;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;

public class ObjectParserTest {

    private static Path scratch;
    private static ObjectParser impl = null;

    @BeforeClass
    public static void prepare() throws Exception {

        scratch = Paths.get(TestCommon.getTempDir(), "test_object_parser");
        impl = new ObjectParser();
    }

    @AfterClass
    public static void teardown() throws Exception {

        final boolean deleteTempFiles = TestCommon.getDeleteTempFiles();

        if (scratch != null && scratch.toFile().exists() && deleteTempFiles) {
            FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    @Test
    public void prepareTempFileTest() throws Exception {

        final File tempDir = new File(scratch.toString(), UUID.randomUUID().toString());
        assertEquals(false, tempDir.exists());

        @SuppressWarnings("static-access")
        final File tmpFile = impl.prepareTempFile(tempDir);
        assertEquals(true, tempDir.exists());
        assertEquals(true, tmpFile.exists());

        assertThat(tmpFile.toString(),
                both(containsString("ws_srv_response_")).and(containsString(".json")));
    }

    @Test
    public void extractParentFragmentGenomeFeatureTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "GenomeFeature";

        final String parentJson = extractParentFragment(type, jsonResource);
        final String expectedParentJson = "{\"domain\":\"B\",\"scientific_name\":\"Shewanella\","
                + "\"assembly_ref\":\"1/2/1\"}";
        assertEquals(parentJson, expectedParentJson);
    }

    @Test
    public void extractParentFragmentGenomeTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "Genome";

        final String parentJson = extractParentFragment(type, jsonResource);
        assertNull(parentJson); // Genome.json doesn't have "from-parent" path
    }

    public static String extractParentFragment(String type, String jsonResource) throws Exception {

        @SuppressWarnings("unchecked")
        final Map<String, Object> parsingRulesObj = UObject.getMapper()
                .readValue(new File("resources/types/" + type + ".json"), Map.class);
        final ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules
                .fromObject(parsingRulesObj, "test");

        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            final String parentJson = ObjectParser.extractParentFragment(parsingRules, jts);
            return parentJson;
        }
    }

    @Test
    public void prepareGUIDGenomeFeatureTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "GenomeFeature";

        final ArrayList<GUID> idList = prepareGUID(type, jsonResource, new GUID("WS:1/1/1"));

        assertThat(idList.size(), is(3));
        final List<String> expectedIds = Arrays.asList("NewGenome.CDS.6211", "NewGenome.CDS.6210",
                "NewGenome.CDS.6209");
        for (GUID guid : idList) {
            assertThat(guid.getSubObjectId(), is(expectedIds.get(idList.indexOf(guid))));
            assertThat(guid.getSubObjectType(), is("feature"));
            assertThat(guid.getStorageCode(), is("WS"));
            assertThat(guid.getVersion(), is(1));
            assertThat(guid.getAccessGroupId(), is(1));
            assertThat(guid.getAccessGroupObjectId(), is("1"));
        }
    }

    public static ArrayList<GUID> prepareGUID(String type, String jsonResource, GUID ref)
            throws Exception {

        final ArrayList<GUID> idList = new ArrayList<GUID>();

        @SuppressWarnings("unchecked")
        final Map<String, Object> parsingRulesObj = UObject.getMapper()
                .readValue(new File("resources/types/" + type + ".json"), Map.class);
        ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj,
                "test");
        final Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        final SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);

        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
        }

        for (ObjectJsonPath path : pathToJson.keySet()) {
            final String subJson = pathToJson.get(path);
            final SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getPrimaryKeyPath() != null
                    || parsingRules.getRelationRules() != null) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(),
                            parsingRules.getRelationRules(), subJts, idConsumer);
                }
            }
            final GUID id = ObjectParser.prepareGUID(parsingRules, ref, path, idConsumer);
            idList.add(id);
        }

        return idList;
    }

    @Test
    public void parseSubObjectsGenomeTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "Genome";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(1));
        final String jsonString = guidToJson.values().iterator().next();
        @SuppressWarnings("unchecked")
        HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);

        HashMap<String, Object> expectedResult = new HashMap<String, Object>();
        expectedResult.put("features", 3);
        expectedResult.put("domain", "B");
        expectedResult.put("assembly_ref", "1/2/1");
        expectedResult.put("scientific_name", "Shewanella");
        expectedResult.put("id", "NewGenome");

        assertThat(expectedResult, is(result));
    }

    @Test
    public void parseSubObjectsGenomeFeatureTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "GenomeFeature";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(3));

        final List<String> expectedIds = Arrays.asList("NewGenome.CDS.6211", "NewGenome.CDS.6210",
                "NewGenome.CDS.6209");
        int key_index = 0;
        for (GUID guidKey : guidToJson.keySet()) {
            assertThat(guidKey.getSubObjectId(), is(expectedIds.get(key_index)));
            assertThat(guidKey.getSubObjectType(), is("feature"));
            assertThat(guidKey.getStorageCode(), is("WS"));
            assertThat(guidKey.getVersion(), is(1));
            assertThat(guidKey.getAccessGroupId(), is(1));
            assertThat(guidKey.getAccessGroupObjectId(), is("1"));
            key_index++;
        }

        final List<String> expectedKeys = Arrays.asList("ontology_terms", "function",
                "protein_translation", "location", "id", "type");
        final List<String> expectedFunctions = Arrays.asList("Polysaccharide biosynthesis protein",
                "Transcriptional activator RfaH", "Di-tripeptide/cation symporter");
        final String expectedType = "CDS";
        int value_index = 0;
        for (String jsonString : guidToJson.values()) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> result = new ObjectMapper().readValue(jsonString,
                    HashMap.class);
            assertThat(expectedKeys.containsAll(result.keySet()), is(true));
            assertThat(result.get("id"), is(expectedIds.get(value_index)));
            assertThat(result.get("function"), is(expectedFunctions.get(value_index)));
            assertThat(result.get("type"), is(expectedType));
            value_index++;
        }
    }

    public static Map<GUID, String> parseSubObjects(String type, String jsonResource,
            String guidString) throws Exception {

        final GUID guid = new GUID(guidString);

        final InputStream inputStream = ObjectParserTest.class
                .getResourceAsStream(jsonResource + ".json.properties");
        final Reader reader = new InputStreamReader(inputStream);

        final UObject data = UObject.fromJsonString(CharStreams.toString(reader));
        final String name = "TestObj" + "_" + type;
        final String creator = "creator";

        final SourceData obj = SourceData.getBuilder(data, name, creator).build();

        @SuppressWarnings("unchecked")
        final Map<String, Object> parsingRulesObj = UObject.getMapper()
                .readValue(new File("resources/types/" + type + ".json"), Map.class);
        final ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules
                .fromObject(parsingRulesObj, "test");

        final Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, guid, parsingRules);

        return guidToJson;
    }
}
