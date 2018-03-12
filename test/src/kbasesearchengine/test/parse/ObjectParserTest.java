package kbasesearchengine.test.parse;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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
import kbasesearchengine.events.handler.SourceData.Builder;
import kbasesearchengine.parse.IdMapper;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.SimpleIdConsumer;
import kbasesearchengine.parse.SimpleSubObjectConsumer;
import kbasesearchengine.parse.SubObjectConsumer;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;

public class ObjectParserTest {

    private static Path scratch;

    @BeforeClass
    public static void prepare() throws Exception {

        scratch = Paths.get(TestCommon.getTempDir(), "test_object_parser");
    }

    @AfterClass
    public static void teardown() throws Exception {

        final boolean deleteTempFiles = TestCommon.getDeleteTempFiles();

        if (scratch != null && scratch.toFile().exists() && deleteTempFiles) {
            FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    /**
     * Testing object with 'from-parent' attributes
     * 
     * @throws Exception
     */
    @Test
    public void extractParentFragmentGenomeFeatureTest() throws Exception {

        final String jsonResource = "genome01";
        final String type = "GenomeFeature";

        final String parentJson = extractParentFragment(type, jsonResource);
        final String expectedParentJson = "{\"domain\":\"B\",\"scientific_name\":\"Shewanella\","
                + "\"assembly_ref\":\"1/2/1\"}";
        assertEquals(parentJson, expectedParentJson);
    }

    /**
     * Testing object with no 'from-parent' attributes
     * 
     * @throws Exception
     */
    @Test
    public void extractParentFragmentGenomeTest() throws Exception {

        final String jsonResource = "assembly01";
        final String type = "Assembly";

        final String parentJson = extractParentFragment(type, jsonResource);
        assertNull(parentJson); // Assembly.yaml doesn't have "from-parent" path
    }

    /**
     * Helper method for ObjectParser.extractParentFragment tests
     * 
     * @param type
     * @param jsonResource
     * @return
     * @throws Exception
     */
    public static String extractParentFragment(String type, String jsonResource) throws Exception {

        final File rulesFile = new File("resources/types/" + type + ".yaml");
        final ObjectTypeParsingRules parsingRules = ObjectTypeParsingRulesFileParser
                .fromFile(rulesFile).get(0);

        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            final String parentJson = ObjectParser.extractParentFragment(parsingRules, jts);
            return parentJson;
        }
    }

    /**
     * Testing preparing a genome feature GUID
     * 
     * @throws Exception
     */
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

    /**
     * Helper method for ObjectParser.prepareGUID tests
     * 
     * @param type
     * @param jsonResource
     * @param ref
     * @return
     * @throws Exception
     */
    public static ArrayList<GUID> prepareGUID(String type, String jsonResource, GUID ref)
            throws Exception {

        final ArrayList<GUID> idList = new ArrayList<GUID>();

        final File rulesFile = new File("resources/types/" + type + ".yaml");
        final ObjectTypeParsingRules parsingRules = ObjectTypeParsingRulesFileParser
                .fromFile(rulesFile).get(0);
        final Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        final SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);

        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
            ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
        }

        for (ObjectJsonPath path : pathToJson.keySet()) {
            final String subJson = pathToJson.get(path);
            final SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getSubObjectIDPath().isPresent()) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getSubObjectIDPath().get(), subJts, idConsumer);
                }
            }
            final GUID id = ObjectParser.prepareGUID(parsingRules, ref, path, idConsumer);
            idList.add(id);
        }

        return idList;
    }

    /**
     * Testing parsing Assembly object
     * 
     * @throws Exception
     */
    @Test
    public void parseSubObjectsGenomeTest() throws Exception {

        final String jsonResource = "assembly01";
        final String type = "Assembly";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(1));
        final String jsonString = guidToJson.values().iterator().next();
        @SuppressWarnings("unchecked")
        HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);

        HashMap<String, Object> expectedResult = new HashMap<String, Object>();
        expectedResult.put("dna_size", 4706287);
        expectedResult.put("external_source_id", "NC_008321.fna");
        expectedResult.put("contigs", 1);
        expectedResult.put("name", "test_asmb.1");
        expectedResult.put("gc_content", 0.478944654246543);

        assertThat(expectedResult, is(result));
    }

    /**
     * Testing parsing GenomeFeature object
     * 
     * @throws Exception
     */
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

    /**
     * Testing parsing MediaCompound object
     *
     * @throws Exception
     */
    @Test
    public void parseSubObjectsMediaCompoundTest() throws Exception {

        final String jsonResource = "media01";
        final String type = "MediaCompound";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(3));

        final List<String> expectedIds = Arrays.asList(
                "48/1/1/compounds/id/cpd00027",
                "48/1/1/compounds/id/cpd00013",
                "48/1/1/compounds/id/cpd00009");
        int key_index = 0;
        for (GUID guidKey : guidToJson.keySet()) {
            assertThat(guidKey.getSubObjectId(), is(expectedIds.get(key_index)));
            assertThat(guidKey.getSubObjectType(), is("MediaCompound"));
            assertThat(guidKey.getStorageCode(), is("WS"));
            assertThat(guidKey.getVersion(), is(1));
            assertThat(guidKey.getAccessGroupId(), is(1));
            assertThat(guidKey.getAccessGroupObjectId(), is("1"));
            key_index++;
        }

        final List<String> expectedKeys = Arrays.asList("inchikey", "concentration",
                "minFlux", "maxFlux", "id", "name", "compound_ref");
        final List<Double> expectedFlux = Arrays.asList( 100.0, 10.0, 50.0);
        final List<String> names = Arrays.asList( "cpd00027", "cpd00013", "cpd00009");
        final Double expectedConcentration = 0.001;
        final String inchikey = "WQZGKKKJIJFFOK-VFUOTHLCSA-N";
        int value_index = 0;
        for (String jsonString : guidToJson.values()) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> result = new ObjectMapper().readValue(jsonString,
                    HashMap.class);
            assertThat(result.keySet().size(), is(expectedKeys.size()));
            assertThat(expectedKeys.containsAll(result.keySet()), is(true));
            assertThat(result.get("compound_ref"), is(expectedIds.get(value_index)));
            assertThat(result.get("id"), is(names.get(value_index)));
            assertThat(result.get("name"), is(names.get(value_index)));
            assertThat(result.get("maxFlux"), is(expectedFlux.get(value_index)));
            assertThat(result.get("minFlux"), is(-expectedFlux.get(value_index)));
            assertThat(result.get("concentration"), is(expectedConcentration));
            assertThat(result.get("inchikey"), is(inchikey));
            value_index++;
        }
    }

    /**
     * Testing parsing RNASeqSampleSet object
     *
     * @throws Exception
     */
    @Test
    public void parseObjectsRNASeqSampleSetTest() throws Exception {

        final String jsonResource = "rnaseqsampleset01";
        final String type = "RNASeqSampleSet";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(1));
        final String jsonString = guidToJson.values().iterator().next();
        @SuppressWarnings("unchecked")
        HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);

        HashMap<String, Object> expectedResult = new HashMap<String, Object>();
        expectedResult.put("sampleset_desc", "Arabidopsis thaliana wild type and hy5-215 seedlings grown under white light.");
        expectedResult.put("source", "NCBI SRP003951");
        expectedResult.put("num_samples", 4);
        expectedResult.put("num_replicates", 3);

        assertThat(result, is(expectedResult));
    }

    /**
     * Testing parsing FBAModel object
     *
     * @throws Exception
     */
    @Test
    public void parseObjectsModelTest() throws Exception {

        final String jsonResource = "fbamodel01";
        final String type = "FBAModel";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(1));
        final String jsonString = guidToJson.values().iterator().next();
        @SuppressWarnings("unchecked")
        HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);

        HashMap<String, Object> expectedResult = new HashMap<String, Object>();
        expectedResult.put("id", "rast_test");
        expectedResult.put("source", "KBase");
        expectedResult.put("modelcompartments", 2);
        expectedResult.put("modelcompounds", 3);
        expectedResult.put("modelreactions", 3);
        expectedResult.put("name", "E. coli");
        expectedResult.put("type", "GenomeScale");
        expectedResult.put("genome_ref", "1/2/1");

        assertThat(result, is(expectedResult));
    }

    /**
     * Testing parsing ModelCompound object
     *
     * @throws Exception
     */
    @Test
    public void parseSubObjectsModelCompoundTest() throws Exception {

        final String jsonResource = "fbamodel01";
        final String type = "ModelCompound";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(3));

        final List<String> expectedIds = Arrays.asList("cpd00001_c0", "cpd02154_c0",
                "cpd00013_c0");
        int key_index = 0;
        for (GUID guidKey : guidToJson.keySet()) {
            assertThat(guidKey.getSubObjectId(), is(expectedIds.get(key_index)));
            assertThat(guidKey.getSubObjectType(), is("ModelCompound"));
            assertThat(guidKey.getStorageCode(), is("WS"));
            assertThat(guidKey.getVersion(), is(1));
            assertThat(guidKey.getAccessGroupId(), is(1));
            assertThat(guidKey.getAccessGroupObjectId(), is("1"));
            key_index++;
        }

        final List<String> expectedKeys = Arrays.asList("inchikey", "formula",
                "aliases", "id", "name");
        final List<String> expectedFormula = Arrays.asList("H2O", "C5H7N2O4", "H4N");
        int value_index = 0;
        for (String jsonString : guidToJson.values()) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> result = new ObjectMapper().readValue(jsonString,
                    HashMap.class);
            assertThat(expectedKeys.containsAll(result.keySet()), is(true));
            assertThat(result.get("id"), is(expectedIds.get(value_index)));
            assertThat(result.get("formula"), is(expectedFormula.get(value_index)));
            value_index++;
        }
    }

    /**
     * Testing parsing ModelReaction object
     *
     * @throws Exception
     */
    @Test
    public void parseSubObjectsModelReactionTest() throws Exception {

        final String jsonResource = "fbamodel01";
        final String type = "ModelReaction";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(3));

        final List<String> expectedIds = Arrays.asList("rxn02282_c0", "rxn02201_c0",
                "rxn00351_c0");
        int key_index = 0;
        for (GUID guidKey : guidToJson.keySet()) {
            assertThat(guidKey.getSubObjectId(), is(expectedIds.get(key_index)));
            assertThat(guidKey.getSubObjectType(), is("ModelReaction"));
            assertThat(guidKey.getStorageCode(), is("WS"));
            assertThat(guidKey.getVersion(), is(1));
            assertThat(guidKey.getAccessGroupId(), is(1));
            assertThat(guidKey.getAccessGroupObjectId(), is("1"));
            key_index++;
        }

        final List<String> expectedKeys = Arrays.asList("pathway", "aliases", "id", 
                "name");
        final List<String> expectedName = Arrays.asList("N-Formimino-L-aspartate iminohydrolase_c0", 
                "2-amino-4-hydroxy-6-hydroxymethyl-7,8-dihydropteridine-diphosphate:4-aminobenzoate 2-amino-4-hydroxydihydropteridine-6-methenyltransferase_c0", 
                "gamma-L-glutamyl-L-cysteine:glycine ligase (ADP-forming)_c0");
        int value_index = 0;
        for (String jsonString : guidToJson.values()) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> result = new ObjectMapper().readValue(jsonString,
                    HashMap.class);
            assertThat(expectedKeys.containsAll(result.keySet()), is(true));
            assertThat(result.get("id"), is(expectedIds.get(value_index)));
            assertThat(result.get("name"), is(expectedName.get(value_index)));
            value_index++;
        }
    }

    /**
     * Testing parsing ModelReactionProteinSubunit object
     *
     * @throws Exception
     */
    @Test
    public void parseSubObjectsModelReactionProteinSubunitTest() throws Exception {

        final String jsonResource = "fbamodel01";
        final String type = "ModelReactionProteinSubunit";
        final String guid = "WS:1/1/1";

        final Map<GUID, String> guidToJson = parseSubObjects(type, jsonResource, guid);

        assertThat(guidToJson.size(), is(3));

        final List<String> expectedIds = Arrays.asList("allantoate deiminase (EC 3.5.3.9)", 
                "Dihydropteroate synthase (EC 2.5.1.15)",
                "Glutathione synthetase (EC 6.3.2.3)");
        int key_index = 0;
        for (GUID guidKey : guidToJson.keySet()) {
            assertThat(guidKey.getSubObjectId(), is(expectedIds.get(key_index)));
            assertThat(guidKey.getSubObjectType(), is("ModelReactionProteinSubunit"));
            assertThat(guidKey.getStorageCode(), is("WS"));
            assertThat(guidKey.getVersion(), is(1));
            assertThat(guidKey.getAccessGroupId(), is(1));
            assertThat(guidKey.getAccessGroupObjectId(), is("1"));
            key_index++;
        }

        final List<String> expectedKeys = Arrays.asList("role", "feature_refs");
        final List<List<String>> expectedName = Arrays.asList(
                Arrays.asList("~/genome/features/id/eco_from_assembly.CDS.522"), 
                Arrays.asList("~/genome/features/id/eco_from_assembly.CDS.3263"), 
                Arrays.asList("~/genome/features/id/eco_from_assembly.CDS.3031"));
        int value_index = 0;
        for (String jsonString : guidToJson.values()) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> result = new ObjectMapper().readValue(jsonString,
                    HashMap.class);
            assertThat(expectedKeys.containsAll(result.keySet()), is(true));
            assertThat(result.get("role"), is(expectedIds.get(value_index)));
            assertThat(result.get("feature_refs"), is(expectedName.get(value_index)));
            value_index++;
        }
    }

    /**
     * Helper method for ObjectParser.parseSubObjects tests
     * 
     * @param type
     * @param jsonResource
     * @param guidString
     * @param rule_version
     * @return
     * @throws Exception
     */
    public static Map<GUID, String> parseSubObjects(String type, String jsonResource,
            String guidString, int rule_version, String... keys) throws Exception {

        final GUID guid = new GUID(guidString);

        final InputStream inputStream = ObjectParserTest.class
                .getResourceAsStream("data/"+jsonResource + ".json.properties");
        final Reader reader = new InputStreamReader(inputStream);

        final UObject data = UObject.fromJsonString(CharStreams.toString(reader));
        final String name = "TestObj" + "_" + type;
        final String creator = "creator";

        final SourceData obj = SourceData.getBuilder(data, name, creator).build();
        final File rulesFile = new File("resources/types/" + type + ".yaml");
        final ObjectTypeParsingRules parsingRules = ObjectTypeParsingRulesFileParser
                .fromFile(rulesFile).get(rule_version);

        final Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, guid, parsingRules);

        return guidToJson;
    }

    /**
     * Helper method for ObjectParser.parseSubObjects tests
     * 
     * @param type
     * @param jsonResource
     * @param guidString
     * @return
     * @throws Exception
     */
    public static Map<GUID, String> parseSubObjects(String type, String jsonResource,
            String guidString) throws Exception {
        return parseSubObjects(type, jsonResource, guidString, 0);
    }
}