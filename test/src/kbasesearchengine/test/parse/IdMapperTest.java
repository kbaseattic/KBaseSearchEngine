package kbasesearchengine.test.parse;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.IdConsumer;
import kbasesearchengine.parse.IdMapper;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableMap;

import us.kbase.common.service.UObject;

public class IdMapperTest {

    @Test
    public void test01() throws Exception {
        System.out.println("test01");
        Map<String, String> data = mapIds("genome01", "/id");
        assertThat("incorrect extracted ids", data, is(ImmutableMap.of("\"NewGenome\"", "!")));
    }

    @Test
    public void test02() throws Exception {
        System.out.println("test02");
        List<String> features = new ArrayList<String>(SubObjectExtractorTest.extractSubObjects(
                "genome01", "/features/[*]", "id", "ontology_terms/*/*/id").values());
        Assert.assertEquals(3, features.size());
        final List<Map<String, String>> expected = Arrays.asList(
                ImmutableMap.of("\"NewGenome.CDS.6211\"", "!"),
                ImmutableMap.of("\"NewGenome.CDS.6210\"", "!"),
                ImmutableMap.of("\"NewGenome.CDS.6209\"", "!"));
        final List<Map<String, String>> got = new LinkedList<>();
        for (String featureText : features) {
            JsonParser jp = UObject.getMapper().getFactory().createParser(featureText);
            got.add(mapIds(jp, "/id"));
        }
        assertThat("incorrect extracted ids", got, is(expected));
    }
    
    @Test
    public void test03() throws Exception {
        System.out.println("test03");
        List<String> locs = new ArrayList<String>(SubObjectExtractorTest.extractSubObjects(
                "genome01", "/features/[*]/location/0", "[*]").values());
        Assert.assertEquals(3, locs.size());
        final List<Map<String, String>> expected = Arrays.asList(
                ImmutableMap.of("169139", "!"),
                ImmutableMap.of("168018", "!"),
                ImmutableMap.of("167592", "!"));
        final List<Map<String, String>> got = new LinkedList<>();
        for (String locText : locs) {
            JsonParser jp = UObject.getMapper().getFactory().createParser(locText);
            got.add(mapIds(jp, "/1"));
        }
        assertThat("incorrect extracted ids", got, is(expected));
    }

    private static Map<String, String> mapIds(
            final String resourceName,
            final String pathToPrimary)
            throws Exception {
        return mapIds(getParsedJsonResource(resourceName), pathToPrimary);
    }
    
    private static Map<String, String> mapIds(
            final JsonParser jp, 
            final String pathToPrimary)
            throws Exception {
        Map<String, String> data = new LinkedHashMap<String, String>();
        IdConsumer consumer = createStringMapConsumer(data);
        IdMapper.mapKeys(new ObjectJsonPath(pathToPrimary), jp, consumer);
        return data;
    }
    
    private static IdConsumer createStringMapConsumer(final Map<String, String> data) {
        return new IdConsumer() {
            @Override
            public void setPrimaryId(Object value) {
                data.put(UObject.transformObjectToString(value), "!");
            }
        };
    }
    
    private static InputStream getJsonResource(String name) throws Exception {
        return IdMapperTest.class.getResourceAsStream("data/" + name + ".json.properties");
    }
    
    private static JsonParser getParsedJsonResource(String name) throws Exception {
        return UObject.getMapper().getFactory().createParser(getJsonResource(name));
    }
}
