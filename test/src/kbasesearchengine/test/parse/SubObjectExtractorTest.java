package kbasesearchengine.test.parse;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.SimpleSubObjectConsumer;
import kbasesearchengine.parse.SubObjectConsumer;
import kbasesearchengine.parse.SubObjectExtractor;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;

import us.kbase.common.service.UObject;

public class SubObjectExtractorTest {

    @Test
    public void test01() throws Exception {
        Map<ObjectJsonPath, String> data = extractSubObjects("genome01", 
                "/features/[*]", "id", "location");
        Assert.assertEquals(3, data.size());
        for (int i = 0; i < data.size(); i++) {
            String featureText = data.get(ObjectJsonPath.path("features", "" + i));
            Assert.assertNotNull(featureText);
            @SuppressWarnings("unchecked")
            Map<String, Object> feature = UObject.transformStringToObject(featureText, Map.class);
            Assert.assertEquals(2, feature.size());
            Assert.assertNotNull(feature.get("id"));
            Assert.assertNotNull(feature.get("location"));
        }
    }

    @Test
    public void test02() throws Exception {
        Map<ObjectJsonPath, String> data = extractSubObjects("genome01", 
                "/", "domain", "gc_content", "genetic_code", "id", "num_contigs",
                "scientific_name", "source", "source_id", "additional");
        Assert.assertEquals(1, data.size());
        for (int i = 0; i < data.size(); i++) {
            String genomeText = data.get(ObjectJsonPath.path());
            Assert.assertNotNull(genomeText);
            @SuppressWarnings("unchecked")
            Map<String, Object> genome = UObject.transformStringToObject(genomeText, Map.class);
            Assert.assertEquals(8, genome.size());
            Assert.assertNotNull(genome.get("id"));
            Assert.assertNotNull(genome.get("scientific_name"));
            Assert.assertEquals(11, genome.get("genetic_code"));
        }
    }

    @Test
    public void test03() throws Exception {
        Map<ObjectJsonPath, String> data = extractSubObjects("genome01", 
                "/features/[*]/location/0", "[*]");
        Assert.assertEquals(3, data.size());
        for (int i = 0; i < data.size(); i++) {
            String locText = data.get(ObjectJsonPath.path("features", "" + i, "location", "0"));
            Assert.assertNotNull(locText);
            @SuppressWarnings("unchecked")
            List<Object> loc = UObject.transformStringToObject(locText, List.class);
            Assert.assertEquals(4, loc.size());
            Assert.assertTrue(loc.get(0) instanceof String);
            Assert.assertTrue(loc.get(1) instanceof Integer);
            Assert.assertTrue(loc.get(2) instanceof String);
            Assert.assertTrue(loc.get(3) instanceof Integer);
        }
    }

    public static Map<ObjectJsonPath, String> extractSubObjects(String resourceName, 
            String pathToSub, String... objPaths) throws Exception {
        JsonParser jp = getParsedJsonResource(resourceName);
        Map<ObjectJsonPath, String> data = new LinkedHashMap<ObjectJsonPath, String>();
        SubObjectConsumer consumer = createStringMapConsumer(data);
        List<ObjectJsonPath> objPaths2 = new ArrayList<ObjectJsonPath>();
        for (String objPath : objPaths) {
            objPaths2.add(new ObjectJsonPath(objPath));
        }
        SubObjectExtractor.extract(new ObjectJsonPath(pathToSub), 
                objPaths2, jp, consumer);
        return data;
    }
    
    private static SubObjectConsumer createStringMapConsumer(final Map<ObjectJsonPath, String> data) {
        return new SimpleSubObjectConsumer(data);
    }
    
    private static InputStream getJsonResource(String name) throws Exception {
        return SubObjectExtractorTest.class.getResourceAsStream("data/"+ name + ".json.properties");
    }
    
    public static JsonParser getParsedJsonResource(String name) throws Exception {
        return UObject.getMapper().getFactory().createParser(getJsonResource(name));
    }
}
