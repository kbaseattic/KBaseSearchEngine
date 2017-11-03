package kbasesearchengine.test.parse;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.both;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.junit.matchers.JUnitMatchers.hasItem;

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
import org.ini4j.Ini;
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
import us.kbase.common.service.UObject;

public class ObjectParserTest {

	private static Map<String, String> config = null;
	private static Path scratch;
	private static ObjectParser impl = null;

	@BeforeClass
	public static void init() throws Exception {
		// config loading
		String configFilePath = System.getenv("KB_DEPLOYMENT_CONFIG");
		if (configFilePath == null) {
			configFilePath = System.getProperty("KB_DEPLOYMENT_CONFIG");
		}
		File deploy = new File(configFilePath);
		Ini ini = new Ini(deploy);
		config = ini.get("KBaseSearchEngine");
		scratch = Paths.get(config.get("scratch"), "test_object_parser");

		impl = new ObjectParser();
	}

	@AfterClass
	public static void teardown() throws Exception {
		FileUtils.deleteDirectory(scratch.toFile());
	}

	@Test
	public void prepareTempFileTest() throws Exception {

		File tempDir = new File(scratch.toString(), UUID.randomUUID().toString());
		assertEquals(false, tempDir.exists());

		@SuppressWarnings("static-access")
		File tmpFile = impl.prepareTempFile(tempDir);
		assertEquals(true, tempDir.exists());
		assertEquals(true, tmpFile.exists());
		
		assertThat(tmpFile.toString(), both(containsString("ws_srv_response_")).and(containsString(".json")));
	}

	@SuppressWarnings("static-access")
	@Test
	public void extractParentFragmentGenomeFeatureTest() throws Exception {

		String jsonResource = "genome01";
		String type = "GenomeFeature";

		String parentJson = this.extractParentFragment(type, jsonResource);
		String expectedParentJson = "{\"domain\":\"B\",\"scientific_name\":\"Shewanella\",\"assembly_ref\":\"1/2/1\"}";
		assertEquals(parentJson, expectedParentJson);
	}

	@SuppressWarnings("static-access")
	@Test
	public void extractParentFragmentGenomeTest() throws Exception {

		String jsonResource = "genome01";
		String type = "Genome";

		String parentJson = this.extractParentFragment(type, jsonResource);
		assertNull(parentJson); // Genome.json doesn't have "from-parent" path
	}

	@SuppressWarnings("unchecked")
	public static String extractParentFragment(String type, String jsonResource) throws Exception {

		Map<String, Object> parsingRulesObj = UObject.getMapper()
				.readValue(new File("resources/types/" + type + ".json"), Map.class);
		ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj, "test");
		
		try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
			String parentJson = ObjectParser.extractParentFragment(parsingRules, jts);
			return parentJson;
		}
	}

	@SuppressWarnings("static-access")
	@Test
	public void prepareGUIDGenomeFeatureTest() throws Exception {

		String jsonResource = "genome01";
		String type = "GenomeFeature";

		List<String> expectedIds = Arrays.asList("NewGenome.CDS.6211", "NewGenome.CDS.6210", "NewGenome.CDS.6209");

		ArrayList<GUID> idList = this.prepareGUID(type, jsonResource, new GUID("WS:1/1/1"));

		assertThat(idList.size(), is(3));
		for (GUID guid : idList) {
			assertThat(expectedIds, hasItem(guid.getSubObjectId()));
			assertThat(guid.getSubObjectType(), is("feature"));
			assertThat(guid.getStorageCode(), is("WS"));
			assertThat(guid.getVersion(), is(1));
			assertThat(guid.getAccessGroupId(), is(1));
			assertThat(guid.getAccessGroupObjectId(), is("1"));
		}
	}

	@SuppressWarnings("unchecked")
	public static ArrayList<GUID> prepareGUID(String type, String jsonResource, GUID ref) throws Exception {

		ArrayList<GUID> idList = new ArrayList<GUID>();

		Map<String, Object> parsingRulesObj = UObject.getMapper()
				.readValue(new File("resources/types/" + type + ".json"), Map.class);
		ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj, "test");
		Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
		SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);

		try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource(jsonResource)) {
			ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
		}

		for (ObjectJsonPath path : pathToJson.keySet()) {
			String subJson = pathToJson.get(path);
			SimpleIdConsumer idConsumer = new SimpleIdConsumer();
			if (parsingRules.getPrimaryKeyPath() != null || parsingRules.getRelationRules() != null) {
				try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
					IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), parsingRules.getRelationRules(), subJts,
							idConsumer);
				}
			}
			GUID id = ObjectParser.prepareGUID(parsingRules, ref, path, idConsumer);
			idList.add(id);
		}

		return idList;
	}

	@SuppressWarnings({ "static-access", "unchecked" })
	@Test
	public void parseSubObjectsGenomeTest() throws Exception {

		String jsonResource = "genome01";
		String type = "Genome";
		String guid = "WS:1/1/1";

		Map<GUID, String> guidToJson = this.parseSubObjects(type, jsonResource, guid);

		assertThat(guidToJson.size(), is(1));
		String jsonString = guidToJson.values().iterator().next();
		HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);
		
		HashMap<String, Object> expectedResult = new HashMap<String, Object>();
		expectedResult.put("features", 3);
		expectedResult.put("domain", "B");
		expectedResult.put("assembly_ref", "1/2/1");
		expectedResult.put("scientific_name", "Shewanella");
		expectedResult.put("id", "NewGenome");
		
		assertThat(expectedResult, is(result));
	}
	
	@SuppressWarnings({ "static-access", "unchecked" })
	@Test
	public void parseSubObjectsGenomeFeatureTest() throws Exception {

		String jsonResource = "genome01";
		String type = "GenomeFeature";
		String guid = "WS:1/1/1";

		Map<GUID, String> guidToJson = this.parseSubObjects(type, jsonResource, guid);

		assertThat(guidToJson.size(), is(3));

		List<String> expectedIds = Arrays.asList("NewGenome.CDS.6211", "NewGenome.CDS.6210", "NewGenome.CDS.6209");
		for (GUID guidKey : guidToJson.keySet()) {
			assertThat(expectedIds, hasItem(guidKey.getSubObjectId()));
			assertThat(guidKey.getSubObjectType(), is("feature"));
			assertThat(guidKey.getStorageCode(), is("WS"));
			assertThat(guidKey.getVersion(), is(1));
			assertThat(guidKey.getAccessGroupId(), is(1));
			assertThat(guidKey.getAccessGroupObjectId(), is("1"));
		}

		List<String> expectedKeys = Arrays.asList("ontology_terms", "function", "protein_translation", "location", "id", "type");
		for (String jsonString : guidToJson.values()) {
			HashMap<String, Object> result = new ObjectMapper().readValue(jsonString, HashMap.class);
			assertThat(expectedKeys.containsAll(result.keySet()), is(true));
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<GUID, String> parseSubObjects(String type, String jsonResource, String guidString)
			throws Exception {

		GUID guid = new GUID(guidString);

		InputStream inputStream = ObjectParserTest.class.getResourceAsStream(jsonResource + ".json.properties");
		Reader reader = new InputStreamReader(inputStream);

		UObject data = UObject.fromJsonString(CharStreams.toString(reader));
        String name = "TestObj" + "_" + type;
        String creator = "creator";
        
        SourceData obj = SourceData.getBuilder(data, name, creator).withNullableCommitHash(null)
                												   .withNullableCopier(null)
													               .withNullableMethod(null)
													               .withNullableModule(null)
													               .withNullableVersion(null)
													               .build();
		
		Map<String, Object> parsingRulesObj = UObject.getMapper()
				.readValue(new File("resources/types/" + type + ".json"), Map.class);
		ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj, "test");

		Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, guid, parsingRules);

		return guidToJson;
	}
}
