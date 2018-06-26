package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static kbasesearchengine.test.common.TestCommon.set;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.NoSuchTypeException;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.TypeMapping;
import kbasesearchengine.system.TypeMappingParser;
import kbasesearchengine.system.TypeParseException;
import kbasesearchengine.system.YAMLTypeMappingParser;
import kbasesearchengine.test.common.TestCommon;

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
                is(Collections.emptySet()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(Collections.emptySet()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(Collections.emptySet()));
                
        verify(logger, never()).logInfo(any());
    }
    
    private static class StreamMatcher implements ArgumentMatcher<InputStream> {

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
            if (is instanceof ResettableInputStream) {
                ((ResettableInputStream) is).reset();
            }
            return expected.equals(contents);
        }
    }
    
    private class ResettableInputStream extends InputStream {
        
        private final String source;
        private InputStream stream;
        
        public ResettableInputStream(final String source) {
            this.source = source;
            this.stream = new ByteArrayInputStream(source.getBytes());
        }
        
        @Override
        public int read() throws IOException {
            return stream.read();
        }
        
        public void reset() {
            stream = new ByteArrayInputStream(source.getBytes());
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
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rule)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rule)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo("[TypeStorage] Skipping file in type tranformation directory: " +
                        "ignore.bar");
        verifyNoMoreInteractions(logger);
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
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(set(rule2)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verifyNoMoreInteractions(logger);
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
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(set(rule1)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo(
                "[TypeStorage] Skipping file in type mapping directory: ignore.json");
        verify(logger).logInfo("[TypeStorage] Overriding type mapping for storage code CD and " +
                "storage type storefoo from type transformation file with definition from " +
                "type mapping file");
        verify(logger).logInfo("[TypeStorage] Processed type mapping file with storage code CD " +
                "and types storefoo.");
        verifyNoMoreInteractions(logger);
    }
    
    @Test
    public void loadVersionsWithVersionMappings() throws Exception {
        // also tests ignoring files that are not regular files
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("foo2.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("foo2.yaml"))).thenReturn(false);
        
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
        final ObjectTypeParsingRules rule3 = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 3),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whoa")).build())
                .build();
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1, rule2, rule3));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Arrays.asList(
                Paths.get("mappings.yaml"), Paths.get("mappings2.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("mappings2.yaml"))).thenReturn(false);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storefoo")
                        .withVersion(2, new SearchObjectType("foo", 1))
                        .withVersion(3, new SearchObjectType("foo", 2))
                        .withDefaultSearchType(new SearchObjectType("foo", 3))
                        .withNullableSourceInfo("map source file")
                        .build()));
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule3)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 3)), is(rule3));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rule3)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rule3)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 3)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 4)),
                is(set(rule3)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(set(rule3)));
                
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo("[TypeStorage] Skipping file in type tranformation directory: " +
                "foo2.yaml");
        verify(logger).logInfo(
                "[TypeStorage] Skipping entry in type mapping directory: mappings2.yaml");
        verify(logger).logInfo("[TypeStorage] Overriding type mapping for storage code CD and " +
                "storage type storefoo from type transformation file with definition from " +
                "type mapping file map source file");
        verify(logger).logInfo("[TypeStorage] Processed type mapping file with storage code CD " +
                "and types storefoo. File: map source file");
        verifyNoMoreInteractions(logger);
    }
    
    @Test
    public void mapNewStorageTypeToSearchType() throws Exception {
        /* Tests the case where a type mapping includes a previously unseen storage type
         * that maps to a search type with a different storage type
         */
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
                Paths.get("mappings.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storebar")
                        .withVersion(1, new SearchObjectType("foo", 1))
                        .withDefaultSearchType(new SearchObjectType("foo", 2))
                        .build()));

        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 3)),
                is(set(rule2)));

        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storebar")),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storebar", 1)),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storebar", 2)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storebar", 3)),
                is(set(rule2)));
        
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                        "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo("[TypeStorage] Processed type mapping file with storage code CD " +
                        "and types storebar.");
        verifyNoMoreInteractions(logger);
    }
    
    @Test
    public void twoSearchTypesOneStorageType() throws Exception {
        /* tests the case where two search types are specified for the same storage type */
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("bar.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("bar.yaml"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml")))
                .thenReturn(new ResettableInputStream("testvaluefoo"));
        final ObjectTypeParsingRules rulefoo = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        
        doReturn(Arrays.asList(rulefoo)).when(typeParser)
                .parseStream(argThat(new StreamMatcher("testvaluefoo")), eq("foo.yaml"));
        
        when(fileLister.newInputStream(Paths.get("bar.yaml")))
                .thenReturn(new ResettableInputStream("testvaluebar"));
        final ObjectTypeParsingRules rulebar = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("bar", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        
        doReturn(Arrays.asList(rulebar)).when(typeParser)
                .parseStream(argThat(new StreamMatcher("testvaluebar")), eq("bar.yaml"));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Collections.emptyList());
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(),
                is(set(rulefoo, rulebar)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rulefoo));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("bar", 1)), is(rulebar));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set(rulefoo, rulebar)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set(rulefoo, rulebar)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rulefoo, rulebar)));
        
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                "code CD, storage type storefoo and search type foo: foo.yaml");
        verify(logger).logInfo("[TypeStorage] Processed type tranformation file with storage " +
                "code CD, storage type storefoo and search type bar: bar.yaml");
        verifyNoMoreInteractions(logger);
    }
    
    @Test
    public void constructFailNulls() {
        final Path t = Paths.get("types");
        final Path m = Paths.get("mappings");
        final ObjectTypeParsingRulesFileParser op = new ObjectTypeParsingRulesFileParser();
        final Map<String, TypeMappingParser> mp = ImmutableMap.of(
                "f", new YAMLTypeMappingParser());
        final FileLister f = new FileLister();
        final LineLogger l = mock(LineLogger.class);
        failConstruct(null, m, op, mp, f, l, new NullPointerException("typesDir"));
        failConstruct(t, null, op, mp, f, l, new NullPointerException("mappingsDir"));
        failConstruct(t, m, null, mp, f, l, new NullPointerException("searchSpecParser"));
        failConstruct(t, m, op, null, f, l, new NullPointerException("mappingParsers"));
        failConstruct(t, m, op, mp, null, l, new NullPointerException("fileLister"));
        failConstruct(t, m, op, mp, f, null, new NullPointerException("logger"));
    }
    
    private void failConstruct(
            final Path typesDir,
            final Path mappingsDir,
            final ObjectTypeParsingRulesFileParser searchSpecParser,
            final Map<String, TypeMappingParser> mappingParsers,
            final FileLister fileLister,
            final LineLogger logger,
            final Exception expected) {
        try {
            new TypeFileStorage(
                    typesDir, mappingsDir, searchSpecParser, mappingParsers, fileLister, logger);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void constructFailSearchTypeCollision() throws Exception {
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("bar.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("bar.yaml"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml")))
                .thenReturn(new ResettableInputStream("testvaluefoo"));
        final ObjectTypeParsingRules rulefoo = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        
        doReturn(Arrays.asList(rulefoo)).when(typeParser)
                .parseStream(argThat(new StreamMatcher("testvaluefoo")), eq("foo.yaml"));
        
        when(fileLister.newInputStream(Paths.get("bar.yaml")))
                .thenReturn(new ResettableInputStream("testvaluebar"));
        final ObjectTypeParsingRules rulebar = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("CD", "storefoo"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee")).build())
                .build();
        
        doReturn(Arrays.asList(rulebar)).when(typeParser)
                .parseStream(argThat(new StreamMatcher("testvaluebar")), eq("bar.yaml"));
        
        failConstruct(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger,
                new TypeParseException("Multiple definitions for the same search type foo in " +
                        "files bar.yaml and foo.yaml"));
    }
    
    @Test
    public void constructFailIOonFileRead() throws Exception {
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("bar.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("bar.yaml"))).thenReturn(true);
        
        when(fileLister.newInputStream(Paths.get("foo.yaml"))).thenThrow(new IOException("ow"));
        
        failConstruct(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger,
                new IOException("ow"));
    }
    
    @Test
    public void constructFailMappingTypeCollision() throws Exception {
        failConstructTypeMappingCollision(null, null,
                "Type collision for type storebar in storage CD.");
        failConstructTypeMappingCollision("source 1", null,
                "Type collision for type storebar in storage CD. (source 1)");
        failConstructTypeMappingCollision(null, "source 2",
                "Type collision for type storebar in storage CD. (source 2)");
        failConstructTypeMappingCollision("source 1", "source 2",
                "Type collision for type storebar in storage CD. (source 1, source 2)");
    }

    private void failConstructTypeMappingCollision(
            final String source1,
            final String source2,
            final String exception)
            throws IOException, ObjectParseException, TypeParseException {
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
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
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Arrays.asList(
                Paths.get("mappings.yaml"), Paths.get("mappings2.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("mappings2.yaml"))).thenReturn(true);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ResettableInputStream("mappingvalue"));
        when(fileLister.newInputStream(Paths.get("mappings2.yaml")))
                .thenReturn(new ResettableInputStream("mappingvalue2"));
        
        doReturn(set(TypeMapping.getBuilder("CD", "storebar")
                        .withVersion(1, new SearchObjectType("foo", 1))
                        .withDefaultSearchType(new SearchObjectType("foo", 1))
                        .withNullableSourceInfo(source1)
                        .build()))
                .when(mappingParser).parse(
                        argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml"));
        doReturn(set(TypeMapping.getBuilder("CD", "storebar")
                        .withVersion(1, new SearchObjectType("foo", 1))
                        .withDefaultSearchType(new SearchObjectType("foo", 1))
                        .withNullableSourceInfo(source2)
                        .build()))
                .when(mappingParser).parse(
                        argThat(new StreamMatcher("mappingvalue2")), eq("mappings2.yaml"));
        
        failConstruct(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger,
                new TypeParseException(exception));
    }

    @Test
    public void constructFailMissingSearchType() throws Exception {
        constructFailMissingSearchType(null, "The search type bar specified in source code/type " +
                "CD/storefoo does not have an equivalent transform type.");
        constructFailMissingSearchType("whee", "The search type bar specified in source " +
                "code/type CD/storefoo does not have an equivalent transform type. File: whee");
    }

    private void constructFailMissingSearchType(
            final String source,
            final String exception)
            throws IOException, ObjectParseException, TypeParseException {
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
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Arrays.asList(
                Paths.get("mappings.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storefoo")
                        .withDefaultSearchType(new SearchObjectType("bar", 1))
                        .withNullableSourceInfo(source)
                        .build()));
        
        failConstruct(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger,
                new TypeParseException(exception));
    }
    
    @Test
    public void constructFailMissingSearchTypeVersion() throws Exception {
        constructFailMissingSearchTypeVersion(null, "Version 2 of search type foo specified in " +
                "source code/type CD/storefoo does not exist.");
        constructFailMissingSearchTypeVersion("whee", "Version 2 of search type foo specified " +
                "in source code/type CD/storefoo does not exist. File: whee");
    }

    private void constructFailMissingSearchTypeVersion(
            final String source,
            final String exception)
            throws IOException, ObjectParseException, TypeParseException {
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
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Arrays.asList(
                Paths.get("mappings.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storefoo")
                        .withDefaultSearchType(new SearchObjectType("foo", 2))
                        .withNullableSourceInfo(source)
                        .build()));
        
        failConstruct(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger,
                new TypeParseException(exception));
    }
    
    @Test
    public void getTypeFailNoType() throws Exception {
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
        
        when(typeParser.parseStream(argThat(new StreamMatcher("testvalue")), eq("foo.yaml")))
                .thenReturn(Arrays.asList(rule1));
        
        when(fileLister.list(Paths.get("mappings"))).thenReturn(Collections.emptyList());
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        
        failGetType(tfs, new SearchObjectType("bar", 1),
                new NoSuchTypeException("No type bar_1 found"));
        failGetType(tfs, new SearchObjectType("foo", 2),
                new NoSuchTypeException("No type foo_2 found"));
    }
    
    private void failGetType(
            final TypeFileStorage tfs,
            final SearchObjectType type,
            final Exception expected) {
        try {
            tfs.getObjectTypeParsingRules(type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void getTypesFromStorageTypeWithoutDefaultMapping() throws Exception {
     // also tests ignoring files that are not regular files
        final ObjectTypeParsingRulesFileParser typeParser =
                mock(ObjectTypeParsingRulesFileParser.class);
        final FileLister fileLister = mock(FileLister.class);
        final TypeMappingParser mappingParser = mock(TypeMappingParser.class);
        final LineLogger logger = mock(LineLogger.class);
        
        when(fileLister.list(Paths.get("types"))).thenReturn(Arrays.asList(
                Paths.get("foo.yaml"), Paths.get("foo2.yaml")));
        when(fileLister.isRegularFile(Paths.get("foo.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("foo2.yaml"))).thenReturn(false);
        
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
                Paths.get("mappings.yaml"), Paths.get("mappings2.yaml")));
        when(fileLister.isRegularFile(Paths.get("mappings.yaml"))).thenReturn(true);
        when(fileLister.isRegularFile(Paths.get("mappings2.yaml"))).thenReturn(false);
        when(fileLister.newInputStream(Paths.get("mappings.yaml")))
                .thenReturn(new ByteArrayInputStream("mappingvalue".getBytes()));
        
        when(mappingParser.parse(argThat(new StreamMatcher("mappingvalue")), eq("mappings.yaml")))
                .thenReturn(set(TypeMapping.getBuilder("CD", "storefoo")
                        .withVersion(2, new SearchObjectType("foo", 1))
                        .withVersion(3, new SearchObjectType("foo", 2))
                        .build()));
        
        final TypeFileStorage tfs = new TypeFileStorage(
                Paths.get("types"),
                Paths.get("mappings"),
                typeParser,
                ImmutableMap.of("yaml", mappingParser),
                fileLister,
                logger);
        
        assertThat("incorrect types", tfs.listObjectTypeParsingRules(), is(set(rule2)));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 1)), is(rule1));
        assertThat("couldn't get type", tfs.getObjectTypeParsingRules(
                new SearchObjectType("foo", 2)), is(rule2));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo")),
                is(set()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 1)),
                is(set()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 2)),
                is(set(rule1)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 3)),
                is(set(rule2)));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 4)),
                is(set()));
        assertThat("object type translation failed",
                tfs.listObjectTypeParsingRules(new StorageObjectType("CD", "storefoo", 7)),
                is(set()));
    }
}
