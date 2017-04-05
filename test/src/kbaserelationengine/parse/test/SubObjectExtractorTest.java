package kbaserelationengine.parse.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import kbaserelationengine.parse.ObjectJsonPath;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.SubObjectConsumer;
import kbaserelationengine.parse.SubObjectExtractor;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import us.kbase.common.service.UObject;

public class SubObjectExtractorTest {

    @Test
    public void test01() throws Exception {
        Map<String, String> data = extractSubObjects("genome01", 
                "/features/[*]", "id", "location");
        Assert.assertEquals(3, data.size());
        for (int i = 0; i < data.size(); i++) {
            String featureText = data.get("/features/" + i);
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
        Map<String, String> data = extractSubObjects("genome01", 
                "/", "domain", "gc_content", "genetic_code", "id", "num_contigs",
                "scientific_name", "source", "source_id", "additional");
        Assert.assertEquals(1, data.size());
        for (int i = 0; i < data.size(); i++) {
            String genomeText = data.get("");
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
        Map<String, String> data = extractSubObjects("genome01", 
                "/features/[*]/location/0", "[*]");
        Assert.assertEquals(3, data.size());
        for (int i = 0; i < data.size(); i++) {
            String locText = data.get("/features/" + i + "/location/0");
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

    public static Map<String, String> extractSubObjects(String resourceName, 
            String pathToSub, String... objPaths) throws Exception {
        JsonParser jp = getParsedJsonResource(resourceName);
        Map<String, String> data = new LinkedHashMap<String, String>();
        SubObjectConsumer consumer = createStringMapConsumer(data);
        List<ObjectJsonPath> objPaths2 = new ArrayList<ObjectJsonPath>();
        for (String objPath : objPaths) {
            objPaths2.add(new ObjectJsonPath(objPath));
        }
        SubObjectExtractor.extract(new ObjectJsonPath(pathToSub), 
                objPaths2, jp, consumer);
        return data;
    }
    
    private static SubObjectConsumer createStringMapConsumer(final Map<String, String> data) {
        return new SubObjectConsumer() {
            private String nextPath = null;
            private StringWriter outChars = null;
            private JsonGenerator nextGen = null;
            
            @Override
            public void nextObject(String path) throws IOException,
                    ObjectParseException {
                flush();
                nextPath = path;
                outChars = new StringWriter();
                nextGen = UObject.getMapper().getFactory().createGenerator(outChars);
            }
            
            @Override
            public JsonGenerator getOutput() throws IOException, ObjectParseException {
                if (nextGen == null) {
                    throw new ObjectParseException("JsonGenerator wasn't initialized");
                }
                return nextGen;
            }
            
            @Override
            public void flush() throws IOException, ObjectParseException {
                if (nextPath != null && nextGen != null && outChars != null) {
                    nextGen.close();
                    data.put(nextPath, outChars.toString());
                    nextPath = null;
                    outChars = null;
                    nextGen = null;
                }
            }
        };
    }
    
    private static InputStream getJsonResource(String name) throws Exception {
        return SubObjectExtractorTest.class.getResourceAsStream(name + ".json.properties");
    }
    
    private static JsonParser getParsedJsonResource(String name) throws Exception {
        return UObject.getMapper().getFactory().createParser(getJsonResource(name));
    }
}
