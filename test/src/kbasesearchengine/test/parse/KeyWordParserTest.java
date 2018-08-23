package kbasesearchengine.test.parse;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import kbasesearchengine.events.handler.ResolvedReference;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.system.NoSuchTypeException;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.parse.ContigLocationException;
import kbasesearchengine.parse.GUIDNotFoundException;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.parse.KeywordParser.ObjectLookupProvider;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.LocationTransformType;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
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
                new ContigLocationException("Expected location array for location transform for " +
                            "CODE:1/2/3:subtype/id, got empty array"));
        
        failLocationTransform(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(Arrays.asList("cid", 1, "+")))),
                new ContigLocationException("Expected location array for location transform for " +
                            "CODE:1/2/3:subtype/id, got [cid, 1, +]"));
        
        failLocationTransform(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "location", Arrays.asList(ImmutableMap.of("foo", "bar")))),
                new ContigLocationException("Expected location array for location transform for " +
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
    
    @Test
    public void guidNotFound() throws Exception {
        final ObjectLookupProvider lookup = mock(ObjectLookupProvider.class);
        
        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "assy_ref", "4/5/6"));
        
        when(lookup.getTypeDescriptor(new SearchObjectType("Assembly", 1)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Assembly", 1),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());
        
        when(lookup.resolveRefs(
                Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(set(new ResolvedReference(new GUID("CODE:4/5/6"), new GUID("CODE:4/5/6"), new StorageObjectType("Code", "Assembly"), Instant.now())));


        
        when(lookup.getTypesForGuids(Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(Collections.emptyMap());

        try {
            KeywordParser.extractKeywords(
                parent,
                new SearchObjectType("Genome", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                    .withTransform(Transform.guid(new SearchObjectType("Assembly", 1)))
                    .build()),
                lookup,
                Arrays.asList(parent));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(
                    got, new GUIDNotFoundException("GUID CODE:4/5/6 not found"));
        }
    }
    
    @Test
    public void unexpectedType() throws Exception {
        final ObjectLookupProvider lookup = mock(ObjectLookupProvider.class);
        
        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "assy_ref", "4/5/6"));
        
        when(lookup.getTypeDescriptor(new SearchObjectType("Assembly", 1)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Assembly", 1),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());
        
        when(lookup.resolveRefs(
                Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(set(new ResolvedReference(new GUID("CODE:4/5/6"), new GUID("CODE:4/5/6"), new StorageObjectType("Code", "Assembly"), Instant.now())));
        
        when(lookup.getTypesForGuids(Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(ImmutableMap.of(
                        new GUID("CODE:4/5/6"), ImmutableList.of(new SearchObjectType("Assembly", 2))));

        try {
            KeywordParser.extractKeywords(
                parent,
                new SearchObjectType("Genome", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                    .withTransform(Transform.guid(new SearchObjectType("Assembly", 1)))
                    .build()),
                lookup,
                Arrays.asList(parent));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(
                    got, new ObjectParseException(
                            "During recursive processing of CODE:1/2/3, " +
                                    "found GUID CODE:4/5/6 has type Assembly v1, " +
                                    "expected one of [SearchObjectType [type=Assembly, version=2]]"));
        }
    }

    /** Tests the case where two or more type spec versions (versions of ObjectTypeParsingRules)
     * exist for a particular storage object type and all contain lookup transforms. The tests checks
     * to ensure that the calls that take place through the ObjectLookupProvider returns
     * the right mappings from StorageObjectType to SearchObjectType.
     *
     * @throws Exception
     *
     */
    @Test
    public void testMultipleSearchSpecVersionsPerGUID() throws Exception {
        final ObjectLookupProvider lookup = mock(ObjectLookupProvider.class);

        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "assy_ref", "4/5/6"));

        // map assembly v1 to KBaseAssembly.Assembly
        when(lookup.getTypeDescriptor(new SearchObjectType("Assembly", 1)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Assembly", 1),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());

        // map assembly v2 to KBaseAssembly.Assembly
        when(lookup.getTypeDescriptor(new SearchObjectType("Assembly", 2)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Assembly", 2),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());

        when(lookup.resolveRefs(
                Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(set(new ResolvedReference(new GUID("CODE:4/5/6"), new GUID("CODE:4/5/6"), new StorageObjectType("Code", "Assembly"), Instant.now())));

        // GUID maps to more than one search type version
        when(lookup.getTypesForGuids(Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(ImmutableMap.of(
                        new GUID("CODE:4/5/6"), ImmutableList.of(
                                new SearchObjectType("Assembly", 1),
                                new SearchObjectType("Assembly", 2)
                        )));


        ParsedObject obj = KeywordParser.extractKeywords(
                parent,
                new SearchObjectType("Genome", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                        .withTransform(Transform.guid(new SearchObjectType("Assembly", 1)))
                        .build()),
                lookup,
                Arrays.asList(parent));
        assertThat("incorrect parsed object", obj.getJson(), is("{\"assy_ref\":\"4/5/6\"}"));

        obj = KeywordParser.extractKeywords(
                parent,
                new SearchObjectType("Genome", 1),
                json,
                null, // parent json
                Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                        .withTransform(Transform.guid(new SearchObjectType("Assembly", 2)))
                        .build()),
                lookup,
                Arrays.asList(parent));
        assertThat("incorrect parsed object", obj.getJson(), is("{\"assy_ref\":\"4/5/6\"}"));
    }

    /** Tests the case where two or more type specs exist for a particular storage
     * object type and all contain lookup transforms. The tests checks to ensure that
     * the calls that take place through the ObjectLookupProvider returns
     * the right mappings from StorageObjectType to SearchObjectType.
     *
     * @throws Exception
     *
     */
    @Test
    public void testMultipleSearchSpecTypesPerGUID() throws Exception {
        final ObjectLookupProvider lookup = mock(ObjectLookupProvider.class);

        final GUID parent = new GUID("CODE:1/2/3");
        final String json = new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "assy_ref", "CODE:4/5/6"));

        // map assembly v1 to KBaseAssembly.Assembly
        when(lookup.getTypeDescriptor(new SearchObjectType("Assembly", 1)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Assembly", 1),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());

        // also map assemblyOther v1 to KBaseAssembly.Assembly
        when(lookup.getTypeDescriptor(new SearchObjectType("AssemblyOther", 1)))
                .thenReturn(ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("AssemblyOther", 1),
                        new StorageObjectType("CODE", "KBaseAssembly.Assembly"))
                        .build());

        final String exceptionMessage = "AssemblyOther version 2 does not exist";

        when(lookup.getTypeDescriptor(new SearchObjectType("AssemblyOther", 2)))
                .thenThrow(new NoSuchTypeException(exceptionMessage));

        when(lookup.resolveRefs(
                Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(set(new ResolvedReference(new GUID("CODE:4/5/6"), new GUID("CODE:4/5/6"), new StorageObjectType("Code", "Assembly"), Instant.now())));

        // GUID maps to more than one search type
        when(lookup.getTypesForGuids(Arrays.asList(new GUID("CODE:1/2/3")), set(new GUID("CODE:4/5/6"))))
                .thenReturn(ImmutableMap.of(
                        new GUID("CODE:4/5/6"), ImmutableList.of(
                                new SearchObjectType("Assembly", 1),
                                new SearchObjectType("AssemblyOther", 1)
                        )));

        {
            // with guid and lookup transform
            ParsedObject obj = KeywordParser.extractKeywords(
                    parent,
                    new SearchObjectType("Genome", 1),
                    json,
                    null, // parent json
                    Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                            .withTransform(Transform.guid(new SearchObjectType("Assembly", 1)))
                            .build()),
                    lookup,
                    Arrays.asList(parent));
            assertThat("incorrect parsed object", obj.getJson(), is("{\"assy_ref\":\"CODE:4/5/6\"}"));
        }

        {
            ParsedObject obj = KeywordParser.extractKeywords(
                    parent,
                    new SearchObjectType("Genome", 1),
                    json,
                    null, // parent json
                    Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                            .withTransform(Transform.guid(new SearchObjectType("AssemblyOther", 1)))
                            .build()),
                    lookup,
                    Arrays.asList(parent));
            assertThat("incorrect parsed object", obj.getJson(), is("{\"assy_ref\":\"CODE:4/5/6\"}"));
        }

        try {
                KeywordParser.extractKeywords(
                        parent,
                        new SearchObjectType("Genome", 1),
                        json,
                        null, // parent json
                        Arrays.asList(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                                .withTransform(Transform.guid(new SearchObjectType("AssemblyOther", 2))) // version 2 does not exist
                                .build()),
                        lookup,
                        Arrays.asList(parent));
        } catch (ObjectParseException ex) {
            assertThat("incorrect exception message", ex.getMessage(), is(exceptionMessage));
        }
    }
}
