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
import kbasesearchengine.system.TypeMappingParser;

public class TypeFileStorageTest {
    
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
        
        final ArgumentMatcher<InputStream> streamMatcher = new ArgumentMatcher<InputStream>() {

            @Override
            public boolean matches(final InputStream isWrapper) {
                final String value;
                try {
                    value = IOUtils.toString(isWrapper);
                } catch (IOException e) {
                    e.printStackTrace();
                    fail("got exception reading inputstream: " + e.getMessage());
                    throw new RuntimeException(); // will never be executed
                }
                assertThat("incorrect input stream contents", value, is("testvalue"));
                return true; // test will have failed if this should be false
            }
        };
        when(typeParser.parseStream(argThat(streamMatcher), eq("foo.yaml")))
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

}
