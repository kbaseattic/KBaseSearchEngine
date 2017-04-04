package kbaserelationengine.parse.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
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
        JsonParser jp = getParsedJsonResource("genome01");
        Map<String, String> data = new LinkedHashMap<String, String>();
        SubObjectConsumer consumer = createStringMapConsumer(data);
        ObjectJsonPath pathToSub = new ObjectJsonPath("/features/[*]");
        List<ObjectJsonPath> objPaths = Arrays.asList(
                new ObjectJsonPath("id"), new ObjectJsonPath("location"));
        SubObjectExtractor.extract(pathToSub, objPaths, jp, consumer);
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
