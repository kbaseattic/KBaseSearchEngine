package kbasesearchengine.test.parse;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.LocationTransformType;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.Transform;
import kbasesearchengine.test.common.TestCommon;

public class KeyWordParserTest {

    //TODO TEST add more tests until the KWP is covered.
    
    //TODO TEST add tests for locations that go over the origin.
    
    @Test
    public void locationTransformSimpleExtractionPosStrandTest() throws Exception {
        locationTransformSimpleExtractionPosStrandTest(
                LocationTransformType.contig_id, "contig_id3");
        locationTransformSimpleExtractionPosStrandTest(LocationTransformType.length, 941);
        locationTransformSimpleExtractionPosStrandTest(LocationTransformType.start, 24);
        locationTransformSimpleExtractionPosStrandTest(LocationTransformType.stop, 964);
        locationTransformSimpleExtractionPosStrandTest(LocationTransformType.strand, "+");
    }
    
    @Test
    public void locationTransformSimpleExtractionNegStrandTest() throws Exception {
        locationTransformSimpleExtractionNegStrandTest(
                LocationTransformType.contig_id, "contig_id3");
        locationTransformSimpleExtractionNegStrandTest(LocationTransformType.length, 941);
        locationTransformSimpleExtractionNegStrandTest(LocationTransformType.start, 24);
        locationTransformSimpleExtractionNegStrandTest(LocationTransformType.stop, 964);
        locationTransformSimpleExtractionNegStrandTest(LocationTransformType.strand, "-");
    }

    private void locationTransformSimpleExtractionPosStrandTest(
            final LocationTransformType locationType,
            final Object expectedKey)
            throws JsonProcessingException, IOException, ObjectParseException,
            IndexingException, InterruptedException {
        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(Arrays.asList("contig_id3", 24, "+", 941))));
        
        final ParsedObject got = KeywordParser.extractKeywords(
                new GUID(parent, "subtype", "id"),
                new SearchObjectType("searchType", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("location"))
                    .withTransform(Transform.location(locationType))
                    .withKeyName("newkey")
                    .build()),
                null, // look up provider
                Arrays.asList(parent));
        
        final ParsedObject expected = new ParsedObject(json, ImmutableMap.of(
                "newkey", Arrays.asList(expectedKey)));
        
        assertThat("incorrect parsed obj", got, is(expected));
    }
    
    private void locationTransformSimpleExtractionNegStrandTest(
            final LocationTransformType locationType,
            final Object expectedKey)
            throws JsonProcessingException, IOException, ObjectParseException,
            IndexingException, InterruptedException {
        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(Arrays.asList("contig_id3", 964, "-", 941))));
        
        final ParsedObject got = KeywordParser.extractKeywords(
                new GUID(parent, "subtype", "id"),
                new SearchObjectType("searchType", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("location"))
                    .withTransform(Transform.location(locationType))
                    .withKeyName("newkey")
                    .build()),
                null, // look up provider
                Arrays.asList(parent));
        
        final ParsedObject expected = new ParsedObject(json, ImmutableMap.of(
                "newkey", Arrays.asList(expectedKey)));
        
        assertThat("incorrect parsed obj", got, is(expected));
    }
    
    @Test
    public void locationTransformFail() throws Exception {
        // why are there multiple arrays anyway...?
        failLocationTransform(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Collections.emptyList())),
                new ObjectParseException("Expected location array for location transform for " +
                            "CODE:1/2/3:subtype/id, got empty array"));
        
        failLocationTransform(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(Arrays.asList("cid", 1, "+")))),
                new ObjectParseException("Expected location array for location transform for " +
                            "CODE:1/2/3:subtype/id, got [cid, 1, +]"));
        
        failLocationTransform(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(ImmutableMap.of("foo", "bar")))),
                new ObjectParseException("Expected location array for location transform for " +
                            "CODE:1/2/3:subtype/id, got [{foo=bar}]"));
    }
    
    private void failLocationTransform(final String json, final Exception expected) {
        
        try {
            final GUID parent = new GUID("CODE:1/2/3");
            
            KeywordParser.extractKeywords(
                    new GUID(parent, "subtype", "id"),
                    new SearchObjectType("searchType", 1),
                    json,
                    null, // parent json
                    Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("location"))
                        .withTransform(Transform.location(LocationTransformType.contig_id))
                        .withKeyName("newkey")
                        .build()),
                    null, // look up provider
                    Arrays.asList(parent));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
