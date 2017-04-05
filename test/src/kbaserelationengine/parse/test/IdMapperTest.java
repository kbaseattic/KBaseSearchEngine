package kbaserelationengine.parse.test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import kbaserelationengine.parse.IdConsumer;
import kbaserelationengine.parse.IdMapper;
import kbaserelationengine.parse.ObjectJsonPath;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;

import us.kbase.common.service.UObject;

public class IdMapperTest {

    @Test
    public void test01() throws Exception {
        String ftKeyType = "Feature";
        String expectedGenomeId = "NewGenome";
        Map<String, String> data = mapIds("genome01", 
                "/id", "/features/[*]/id", ftKeyType);
        Assert.assertEquals(4, data.size());
        Assert.assertTrue(data.containsKey("\"" + expectedGenomeId + "\""));
        Assert.assertEquals("!", data.get("\"" + expectedGenomeId + "\""));
        data.remove("\"" + expectedGenomeId + "\"");
        for (String id : data.keySet()) {
            Assert.assertTrue(id.startsWith("\"" + expectedGenomeId + ".CDS."));
            Assert.assertEquals(ftKeyType, data.get(id));
        }
    }

    @Test
    public void test02() throws Exception {
        String otKeyType = "OntologyTerm";
        List<String> features = new ArrayList<String>(SubObjectExtractorTest.extractSubObjects(
                "genome01", "/features/[*]", "id", "ontology_terms/*/*/id").values());
        String expectedSsoId = "SSO:000008186";
        String ssoIdType = null;
        Assert.assertEquals(3, features.size());
        for (String featureText : features) {
            JsonParser jp = UObject.getMapper().getFactory().createParser(featureText);
            Map<String, String> data = mapIds(jp, 
                    "/id", "/ontology_terms/*/*/id", otKeyType);
            if (data.containsKey("\"" + expectedSsoId + "\"")) {
                ssoIdType = data.get("\"" + expectedSsoId + "\"");
                data.remove("\"" + expectedSsoId + "\"");
            }
            Assert.assertEquals(1, data.size());
            Assert.assertEquals("!", data.get(data.keySet().iterator().next()));
        }
        Assert.assertEquals(otKeyType, ssoIdType);
    }
    
    @Test
    public void test03() throws Exception {
        String ctgKeyType = "ContigId";
        List<String> locs = new ArrayList<String>(SubObjectExtractorTest.extractSubObjects(
                "genome01", "/features/[*]/location/0", "[*]").values());
        Assert.assertEquals(3, locs.size());
        for (String locText : locs) {
            JsonParser jp = UObject.getMapper().getFactory().createParser(locText);
            Map<String, String> data = mapIds(jp, 
                    "/1", "/0", ctgKeyType);
            Assert.assertEquals(2, data.size());
            for (String idJson : data.keySet()) {
                Object idObj = UObject.transformStringToObject(idJson, Object.class);
                String idType = data.get(idJson);
                if (idType.equals("!")) {
                    Assert.assertTrue(idObj instanceof Integer);
                } else {
                    Assert.assertTrue(idObj instanceof String);
                    Assert.assertEquals(ctgKeyType, idType);
                }
            }
        }
    }

    private static Map<String, String> mapIds(String resourceName, 
            String pathToPrimary, String... foreignKeyPathToTypePairs) throws Exception {
        return mapIds(getParsedJsonResource(resourceName), pathToPrimary, 
                foreignKeyPathToTypePairs);
    }
    
    private static Map<String, String> mapIds(JsonParser jp, 
            String pathToPrimary, String... foreignKeyPathToTypePairs) throws Exception {
        Map<String, String> data = new LinkedHashMap<String, String>();
        IdConsumer consumer = createStringMapConsumer(data);
        Map<ObjectJsonPath, String> foreignKeyPathToType = new LinkedHashMap<>();
        for (int i = 0; i < foreignKeyPathToTypePairs.length / 2; i++) {
            String idPath = foreignKeyPathToTypePairs[i * 2];
            String keyType = foreignKeyPathToTypePairs[i * 2 + 1];
            foreignKeyPathToType.put(new ObjectJsonPath(idPath), keyType);
        }
        IdMapper.mapKeys(new ObjectJsonPath(pathToPrimary), 
                foreignKeyPathToType, jp, consumer);
        return data;
    }
    
    private static IdConsumer createStringMapConsumer(final Map<String, String> data) {
        return new IdConsumer() {
            @Override
            public void setPrimaryId(Object value) {
                data.put(UObject.transformObjectToString(value), "!");
            }
            @Override
            public void addForeignKeyId(String keyType, Object value) {
                data.put(UObject.transformObjectToString(value), keyType);
            }
        };
    }
    
    private static InputStream getJsonResource(String name) throws Exception {
        return IdMapperTest.class.getResourceAsStream(name + ".json.properties");
    }
    
    private static JsonParser getParsedJsonResource(String name) throws Exception {
        return UObject.getMapper().getFactory().createParser(getJsonResource(name));
    }
}
