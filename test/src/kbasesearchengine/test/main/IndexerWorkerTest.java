package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.exceptions.UnprocessableEventIndexingException;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;

public class IndexerWorkerTest {
    
    //TODO TEST add more worker tests
    
    /* these are strictly unit tests. */
    
    @Test
    public void idManglingBugPass() throws Exception {
        /* tests a bug where subobject ids would be mangled when primary-key-path was not
         * present or incorrect in the spec, or there was no indexing rule that caused the
         * primary-key-path part of the subobject to be extracted.
         * This is a happy path test where everything is right.
         */
        
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of("id", "an id", "somedata", "data"),
                        ImmutableMap.of("id", "an id2", "somedata", "data2")
                        )
                );
        
        final EventHandler ws = mock(EventHandler.class);
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final IndexingStorage idxStore = mock(IndexingStorage.class);
        final TypeStorage typeStore = mock(TypeStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final Path tempDir = Paths.get(TestCommon.getTempDir()).toAbsolutePath()
                .resolve("IndexerWorkerTest");
        deleteRecursively(tempDir);
        
        when(ws.getStorageCode()).thenReturn("code");
        
        final IndexerWorker worker = new IndexerWorker(
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger);
        
        final GUID guid = new GUID("code:1/2/3");
        when(idxStore.checkParentGuidsExist(set(guid))).thenReturn(ImmutableMap.of(guid, false));
        
        when(ws.load(eq(Arrays.asList(guid)), any(Path.class)))
                .thenAnswer(new Answer<SourceData>() {

                        @Override
                        public SourceData answer(final InvocationOnMock inv) throws Throwable {
                            final Path path = inv.getArgument(1);
                            new ObjectMapper().writeValue(path.toFile(), data);
                            return SourceData.getBuilder(
                                    new UObject(path.toFile()), "myobj", "somedude")
                                    .withNullableMD5("md5")
                                    .build();
                        }
        });

        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "sometype", 3);
        
        when(typeStore.listObjectTypesByStorageObjectType(storageObjectType))
                .thenReturn(Arrays.asList(
                        ObjectTypeParsingRules.getBuilder("foo", storageObjectType)
                                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                                        new ObjectJsonPath("id"))
                                .withIndexingRule(IndexingRules.fromPath(
                                        new ObjectJsonPath("somedata"))
                                        .build())
                                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                                        .build())
                                .build()));
        
        worker.processOneEvent(StatusEvent.getBuilder(
                storageObjectType,
                Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build());
        
        final ParsedObject po1 = new ParsedObject();
        po1.json = new ObjectMapper().writeValueAsString(
                ImmutableMap.of( "id", "an id", "somedata", "data"));
        po1.keywords = ImmutableMap.of("somedata", Arrays.asList("data"),
                "id", Arrays.asList("an id"));
        final ParsedObject po2 = new ParsedObject();
        po2.json = new ObjectMapper().writeValueAsString(
                ImmutableMap.of("id", "an id2", "somedata", "data2"));
        po2.keywords = ImmutableMap.of("somedata", Arrays.asList("data2"),
                "id", Arrays.asList("an id2"));
        
        verify(idxStore).indexObjects(
                eq("foo"),
                any(SourceData.class),
                eq(Instant.ofEpochMilli(10000)),
                eq(null),
                eq(guid),
                eq(ImmutableMap.of(
                        new GUID(guid, "subfoo", "an id2"), po2,
                        new GUID(guid, "subfoo", "an id"), po1)),
                eq(false),
                eq(Arrays.asList(
                        IndexingRules.fromPath(new ObjectJsonPath("somedata")).build(),
                        IndexingRules.fromPath(new ObjectJsonPath("id")).build())));
    }
    
    @Test
    public void idManglingBugFailBadID() throws Exception {
        /* tests a bug where subobject ids would be mangled when primary-key-path was not
         * present or incorrect in the spec, or there was no indexing rule that caused the
         * primary-key-path part of the subobject to be extracted.
         * This tests an incorrect sub object id.
         */
        
        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "sometype", 3);
        final ObjectTypeParsingRules rules = ObjectTypeParsingRules.getBuilder(
                "foo", storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("idx"))
                .withIndexingRule(IndexingRules.fromPath(
                        new ObjectJsonPath("somedata"))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                        .build())
                .build();
        idManglingBugFail(storageObjectType, rules);
    }
    
    @Test
    public void idManglingBugFailNoIndexingRule() throws Exception {
        /* tests a bug where subobject ids would be mangled when primary-key-path was not
         * present or incorrect in the spec, or there was no indexing rule that caused the
         * primary-key-path part of the subobject to be extracted.
         * This tests a missing indexing rule for a sub object id.
         */
        
        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "sometype", 3);
        final ObjectTypeParsingRules rules = ObjectTypeParsingRules.getBuilder(
                "foo", storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(
                        new ObjectJsonPath("somedata"))
                        .build())
                .build();
        idManglingBugFail(storageObjectType, rules);
    }

    private void idManglingBugFail(
            final StorageObjectType storageObjectType,
            final ObjectTypeParsingRules rules)
            throws Exception {
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of("id", "an id", "somedata", "data"),
                        ImmutableMap.of("id", "an id2", "somedata", "data2")
                        )
                );
        
        final EventHandler ws = mock(EventHandler.class);
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final IndexingStorage idxStore = mock(IndexingStorage.class);
        final TypeStorage typeStore = mock(TypeStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final Path tempDir = Paths.get(TestCommon.getTempDir()).toAbsolutePath()
                .resolve("IndexerWorkerTest");
        deleteRecursively(tempDir);
        
        when(ws.getStorageCode()).thenReturn("code");
        
        final IndexerWorker worker = new IndexerWorker(
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger);
        
        final GUID guid = new GUID("code:1/2/3");
        when(idxStore.checkParentGuidsExist(set(guid))).thenReturn(ImmutableMap.of(guid, false));
        
        when(ws.load(eq(Arrays.asList(guid)), any(Path.class)))
                .thenAnswer(new Answer<SourceData>() {

                        @Override
                        public SourceData answer(final InvocationOnMock inv) throws Throwable {
                            final Path path = inv.getArgument(1);
                            new ObjectMapper().writeValue(path.toFile(), data);
                            return SourceData.getBuilder(
                                    new UObject(path.toFile()), "myobj", "somedude")
                                    .withNullableMD5("md5")
                                    .build();
                        }
        });

        when(typeStore.listObjectTypesByStorageObjectType(storageObjectType))
                .thenReturn(Arrays.asList(
                        rules));
        try {
            worker.processOneEvent(StatusEvent.getBuilder(
                    storageObjectType,
                    Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                    .withNullableAccessGroupID(1)
                    .withNullableObjectID("2")
                    .withNullableVersion(3)
                    .withNullableisPublic(false)
                    .build());
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new UnprocessableEventIndexingException(
                    "Could not find the subobject id for one or more of the subobjects for " +
                    "object code:1/2/3 when applying search specification foo"));
        }
    }
    
    private void deleteRecursively(final Path path) throws Exception {
        // https://stackoverflow.com/a/35989142/643675
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

}
