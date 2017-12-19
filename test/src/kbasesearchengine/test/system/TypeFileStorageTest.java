package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static kbasesearchengine.test.common.TestCommon.set;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.TypeMapping;
import kbasesearchengine.system.TypeMappingParser;

public class TypeFileStorageTest {
    
    @Test
    public void noFiles() throws Exception {
        
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Collections.emptyList());
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Collections.emptyList());
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                Collections.emptyMap(),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(),
                is(Collections.emptyList()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(Collections.emptyList()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(Collections.emptyList()));
                
        verify(logger, never()).logInfo(any());
    }
    
    private class StreamMatcher implements ArgumentMatcher<InputStream> {

        private final String expected;
        
        public StreamMatcher(final String expectedStreamContents) {
            this.expected = expectedStreamContents;
        }
        
        @Override
        public boolean matches(final InputStream is) {
            final String contents;
            try {
                contents = IOUtils.toString(is);
            } catch (IOException e) {
                e.printStackTrace();
                fail("got exception reading inputstream: " + e.getMessage());
                throw new RuntimeException(); // will never be executed
            }
            assertThat("incorrect input stream contents", contents, is(expected));
            return true; // test will have failed if this should be false
        }
        
    }
    
    @Test
    public void loadSingleType() throws Exception {
        
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final TypeMappingParser mappingParser1 = mock(TypeMappingParser.class);
        final TypeMappingParser mappingParser2 = mock(TypeMappingParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("ignore.bar")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("ignore.bar"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml")))
                .thenReturn(new ByteArrayInputStream("testvalue".getBytes()));
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Collections.emptyList());
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser1, "foo", mappingParser2),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(Arrays.asList(rule)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(Arrays.asList(rule)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(Arrays.asList(rule)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo("[TypeStorage] Skipping file in type tranformation directory: " +
                        "ignore.bar");
        verify(mappingParser1, never()).parse(any(), any());
        verify(mappingParser2, never()).parse(any(), any());
    }
    
    @Test
    public void loadVersions() throws Exception {
        
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml")))
                .thenReturn(new ByteArrayInputStream("testvalue".getBytes()));
        final ObjectTypeParsingRules rule1 = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        final ObjectTypeParsingRules rule2 = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 2),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whoo")).build())
                .build();
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1, rule2));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Collections.emptyList());
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                Collections.emptyMap(),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(Arrays.asList(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(Arrays.asList(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(Arrays.asList(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(Arrays.asList(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(Arrays.asList(rule2)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
    }
    
    @Test
    public void loadVersionsWithDefaultMappings() throws Exception {
        
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml")))
                .thenReturn(new ByteArrayInputStream("testvalue".getBytes()));
        final ObjectTypeParsingRules rule1 = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        final ObjectTypeParsingRules rule2 = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 2),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whoo")).build())
                .build();
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1, rule2));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Arrays.asList(
                Paths.get("mappings.yaml"), Paths.get("ignore.json")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("ignore.json"))).thenReturn(true);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storefoo")
                        .withDefaultSearchType(new SearchObjectType("foo", 1))
                        .build()));
                
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(Arrays.asList(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(Arrays.asList(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(Arrays.asList(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(Arrays.asList(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(Arrays.asList(rule1)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo(
                "[TypeStorage] Skipping file in type mapping directory: ignore.json");
        verify(logger).logInfo("[TypeStorage] Overriding type mapping for storage code CD and " +
                "storage type storefoo from type transformation file with definition from " +
                "type mapping file");
    }

}
