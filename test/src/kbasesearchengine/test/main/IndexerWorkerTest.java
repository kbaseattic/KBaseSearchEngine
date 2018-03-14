package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriesExceededIndexingException;
import kbasesearchengine.events.exceptions.UnprocessableEventIndexingException;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.ResolvedReference;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.search.IndexingConflictException;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.LocationTransformType;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.Transform;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;

public class IndexerWorkerTest {
    
    //TODO TEST add more worker tests
    
    /* these are strictly unit tests. */
    
    private class ThrowableMatcher implements ArgumentMatcher<Throwable> {

        private final Throwable expected;
        
        private ThrowableMatcher(final Throwable expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(final Throwable got) {
            try {
                TestCommon.assertExceptionCorrect(got, expected);
            } catch (AssertionError e) {
                // ... and this is a grotesque hack
                return false;
            }
            return true;
        }
        
    }
    
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(
                        new ObjectJsonPath("somedata"))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                        .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final StatusEventProcessingState res = worker.processEvent(
                new ChildStatusEvent(StatusEvent.getBuilder(
                        storageObjectType,
                        Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                        .withNullableAccessGroupID(1)
                        .withNullableObjectID("2")
                        .withNullableVersion(3)
                        .withNullableisPublic(false)
                        .build(),
                        new StatusEventID("parentID")));
        assertThat("incorrect state", res, is(StatusEventProcessingState.INDX));
        
        final ParsedObject po1 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of( "id", "an id", "somedata", "data")),
                ImmutableMap.of("somedata", Arrays.asList("data"),
                        "id", Arrays.asList("an id")));
        final ParsedObject po2 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of("id", "an id2", "somedata", "data2")),
                ImmutableMap.of("somedata", Arrays.asList("data2"),
                        "id", Arrays.asList("an id2")));
        
        verify(idxStore).indexObjects(
                eq(rule),
                any(SourceData.class),
                eq(Instant.ofEpochMilli(10000)),
                eq(null),
                eq(guid),
                eq(ImmutableMap.of(
                        new GUID(guid, "subfoo", "an id2"), po2,
                        new GUID(guid, "subfoo", "an id"), po1)),
                eq(false));
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
                new SearchObjectType("foo", 1), storageObjectType)
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
                new SearchObjectType("foo", 1), storageObjectType)
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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

        when(typeStore.listObjectTypeParsingRules(storageObjectType))
                .thenReturn(set(rules));
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("pid"));
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        verify(logger).logError("Error processing event for event NEW_VERSION with parent ID " +
                "pid: kbasesearchengine.events.exceptions." +
                "UnprocessableEventIndexingException: " +
                "Could not find the subobject id for one or more of the subobjects for " +
                "object code:1/2/3 when applying search specification foo_1");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.OTHER,
                        "Could not find the subobject id for one or more of the subobjects for " +
                                "object code:1/2/3 when applying search specification foo_1"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).store(eq(event), eq("OTHER"), argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.OTHER,
                        "Could not find the subobject id for one or more of the subobjects for " +
                                "object code:1/2/3 when applying search specification foo_1"))));
        
        
    }
    
    @Test
    public void subtypesIndexedFirst() throws Exception {
        /* test that subtypes from an object are sent to the indexing storage first.
         * This ensures that checks against a guid in the storage system mean that the indexing
         * is complete. If subtypes can be indexed after the main object data, the check could
         * pass while subtypes are still being indexed.
         */
        
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of("id", "an id", "somedata", "data"))
                );
        
        final EventHandler ws = mock(EventHandler.class);
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final IndexingStorage idxStore = mock(IndexingStorage.class);
        final TypeStorage typeStore = mock(TypeStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final InOrder idxOrder = inOrder(idxStore);
        
        final Path tempDir = Paths.get(TestCommon.getTempDir()).toAbsolutePath()
                .resolve("IndexerWorkerTest");
        deleteRecursively(tempDir);
        
        when(ws.getStorageCode()).thenReturn("code");
        
        final IndexerWorker worker = new IndexerWorker(
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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
        
        final ObjectTypeParsingRules subrule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("somedata")).build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id")).build())
                .build();
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("thingy")).build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("thingy2")).build())
                .build();
        
        when(typeStore.listObjectTypeParsingRules(storageObjectType))
                .thenReturn(set(rule, subrule));
        
        final StatusEventProcessingState res = worker.processEvent(
                new ChildStatusEvent(StatusEvent.getBuilder(
                        storageObjectType,
                        Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                        .withNullableAccessGroupID(1)
                        .withNullableObjectID("2")
                        .withNullableVersion(3)
                        .withNullableisPublic(false)
                        .build(),
                        new StatusEventID("parentID")));
        assertThat("incorrect state", res, is(StatusEventProcessingState.INDX));
        
        final ParsedObject posub = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of( "id", "an id", "somedata", "data")),
                ImmutableMap.of("somedata", Arrays.asList("data"),
                        "id", Arrays.asList("an id")));
        final ParsedObject po = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of("thingy", 1, "thingy2", "foo")),
                ImmutableMap.of("thingy", Arrays.asList(1),
                        "thingy2", Arrays.asList("foo")));
        
        idxOrder.verify(idxStore).indexObjects(
                eq(subrule),
                any(SourceData.class),
                eq(Instant.ofEpochMilli(10000)),
                eq(null),
                eq(guid),
                eq(ImmutableMap.of(new GUID(guid, "subfoo", "an id"), posub)),
                eq(false));
        
        idxOrder.verify(idxStore).indexObjects(
                eq(rule),
                any(SourceData.class),
                eq(Instant.ofEpochMilli(10000)),
                eq(null),
                eq(guid),
                eq(ImmutableMap.of(guid, po)),
                eq(false));
    }
    
    @Test
    public void indexPassTooManySubobjects() throws Exception {
        /* tests the number of subobjects at the limit does not throw an exception
         */
        
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of("id", "an id", "somedata", "data"),
                        ImmutableMap.of("id", "an id2", "somedata", "data2"),
                        ImmutableMap.of("id", "an id3", "somedata", "data3")
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 3);
        
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
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("somedata"))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                        .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final StatusEventProcessingState res = worker.processEvent(
                new ChildStatusEvent(StatusEvent.getBuilder(
                        storageObjectType,
                        Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                        .withNullableAccessGroupID(1)
                        .withNullableObjectID("2")
                        .withNullableVersion(3)
                        .withNullableisPublic(false)
                        .build(),
                        new StatusEventID("parentID")));
        assertThat("incorrect state", res, is(StatusEventProcessingState.INDX));
        
        final ParsedObject po1 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of( "id", "an id", "somedata", "data")),
                ImmutableMap.of("somedata", Arrays.asList("data"),
                        "id", Arrays.asList("an id")));
        final ParsedObject po2 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of("id", "an id2", "somedata", "data2")),
                ImmutableMap.of("somedata", Arrays.asList("data2"),
                        "id", Arrays.asList("an id2")));
        final ParsedObject po3 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of("id", "an id3", "somedata", "data3")),
                ImmutableMap.of("somedata", Arrays.asList("data3"),
                        "id", Arrays.asList("an id3")));
        
        verify(idxStore).indexObjects(
                eq(rule),
                any(SourceData.class),
                eq(Instant.ofEpochMilli(10000)),
                eq(null),
                eq(guid),
                eq(ImmutableMap.of(
                        new GUID(guid, "subfoo", "an id2"), po2,
                        new GUID(guid, "subfoo", "an id3"), po3,
                        new GUID(guid, "subfoo", "an id"), po1)),
                eq(false));
    }
    
    @Test
    public void indexFailTooManySubobjects() throws Exception {
        /* tests the number of subojects above the limit does throw an exception
         * 
         * also tests recording failures in the db when the event is a StoredStatusEvent vs. a
         * ChildStatusEvent
         */
        
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of("id", "an id", "somedata", "data"),
                        ImmutableMap.of("id", "an id2", "somedata", "data2"),
                        ImmutableMap.of("id", "an id3", "somedata", "data3")
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 2);
        
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
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("somedata"))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                        .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final StoredStatusEvent event = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                storageObjectType,Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("myid"),
                StatusEventProcessingState.PROC)
                .build();
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        verify(logger).logError("Error processing event for event NEW_VERSION myid: " +
                "kbasesearchengine.events.exceptions." +
                "UnprocessableEventIndexingException: Object code:1/2/3 has 3 subobjects, " +
                "exceeding the limit of 2");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.SUBOBJECT_COUNT,
                        "Object code:1/2/3 has 3 subobjects, exceeding the limit of 2"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).setProcessingState(
                eq(new StatusEventID("myid")),
                eq(StatusEventProcessingState.PROC),
                eq("SUBOBJECT_COUNT"),
                argThat(new ThrowableMatcher(new UnprocessableEventIndexingException(
                        ErrorType.SUBOBJECT_COUNT,
                        "Object code:1/2/3 has 3 subobjects, exceeding the limit of 2"))));
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
    
    @Test
    public void conflictOnIndex() throws Exception {
        /* tests the handling of conflict errors when indexing an object. */
        
        //TODO TEST allow configuring the retry time to speed this test up
        
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(
                        new ObjectJsonPath("somedata"))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id"))
                        .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final ParsedObject po1 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of( "id", "an id", "somedata", "data")),
                ImmutableMap.of("somedata", Arrays.asList("data"),
                        "id", Arrays.asList("an id")));
        final ParsedObject po2 = new ParsedObject(
                new ObjectMapper().writeValueAsString(
                        ImmutableMap.of("id", "an id2", "somedata", "data2")),
                ImmutableMap.of("somedata", Arrays.asList("data2"),
                        "id", Arrays.asList("an id2")));
        
        doThrow(new IndexingConflictException("conflict", new IOException("placeholder")))
            .when(idxStore).indexObjects(
                    eq(rule),
                    any(SourceData.class),
                    eq(Instant.ofEpochMilli(10000)),
                    eq(null),
                    eq(guid),
                    eq(ImmutableMap.of(
                            new GUID(guid, "subfoo", "an id2"), po2,
                            new GUID(guid, "subfoo", "an id"), po1)),
                    eq(false));
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("pid"));
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        final String errmsg = "Retriable error in indexer, retry %s: " +
                "kbasesearchengine.events.exceptions.RetriableIndexingException: conflict";
        
        verify(logger).logError(String.format(errmsg, 1));
        verify(logger).logError(String.format(errmsg, 2));
        verify(logger).logError(String.format(errmsg, 3));
        verify(logger).logError(String.format(errmsg, 4));
        verify(logger).logError(String.format(errmsg, 5));

        verify(logger).logError("Error processing event for event NEW_VERSION with parent ID " +
                "pid: kbasesearchengine.events.exceptions." +
                "RetriesExceededIndexingException: " +
                "conflict");

        final RetriableIndexingException retryexception =
                new RetriableIndexingException(ErrorType.INDEXING_CONFLICT, "conflict");
        verify(logger, times(5)).logError(argThat(new ThrowableMatcher(retryexception)));
        verify(logger).logError(argThat(new ThrowableMatcher(
                new RetriesExceededIndexingException(ErrorType.INDEXING_CONFLICT, "conflict"))));
        
        verify(storage).store(eq(event), eq("INDEXING_CONFLICT"), argThat(new ThrowableMatcher(
                new RetriesExceededIndexingException(ErrorType.INDEXING_CONFLICT, "conflict"))));
    }
    
    @Test
    public void conflictOnModify() throws Exception {
        /* tests the handling of conflict errors when modifying an object already in the index.
         * We just test one case rather than having tests that are identical other than which
         * IndexingStorage method throws the error.
         */

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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        doThrow(new IndexingConflictException("conflict", new IOException("placeholder")))
                .when(idxStore).deleteAllVersions(new GUID("WS:3/6"));
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                "WS",Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableAccessGroupID(3)
                .withNullableObjectID("6")
                .build(),
                new StatusEventID("pid"));
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        final String errmsg = "Retriable error in indexer for event DELETE_ALL_VERSIONS with " +
                "parent ID pid, retry %s: " +
                "kbasesearchengine.events.exceptions.RetriableIndexingException: conflict";
        
        verify(logger).logError(String.format(errmsg, 1));
        verify(logger).logError(String.format(errmsg, 2));
        verify(logger).logError(String.format(errmsg, 3));
        verify(logger).logError(String.format(errmsg, 4));
        verify(logger).logError(String.format(errmsg, 5));

        verify(logger).logError("Error processing event for event DELETE_ALL_VERSIONS with " +
                "parent ID pid: kbasesearchengine.events.exceptions." +
                "RetriesExceededIndexingException: " +
                "conflict");

        final RetriableIndexingException retryexception =
                new RetriableIndexingException(ErrorType.INDEXING_CONFLICT, "conflict");
        verify(logger, times(5)).logError(argThat(new ThrowableMatcher(retryexception)));
        verify(logger).logError(argThat(new ThrowableMatcher(
                new RetriesExceededIndexingException(ErrorType.INDEXING_CONFLICT, "conflict"))));
        
        verify(storage).store(eq(event), eq("INDEXING_CONFLICT"), argThat(new ThrowableMatcher(
                new RetriesExceededIndexingException(ErrorType.INDEXING_CONFLICT, "conflict"))));
    }

    @Test
    public void contigLocationError() throws Exception {
        /* tests the handling of missing contig location data when indexing an object. */
        
        final Map<String, Object> data = ImmutableMap.of(
                "thingy", 1,
                "thingy2", "foo",
                "subobjs", Arrays.asList(
                        ImmutableMap.of(
                                "id", "an id",
                                "somedata", "data",
                                "location", Arrays.asList(
                                        Arrays.asList("contig_id3", 24, "+", 941))),
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .toSubObjectRule("subfoo", new ObjectJsonPath("/subobjs/[*]/"),
                        new ObjectJsonPath("id"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("somedata")) .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("location"))
                        .withTransform(Transform.location(LocationTransformType.length))
                        .build())
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("id")) .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("pid"));
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        verify(logger).logError("Error processing event for event NEW_VERSION with parent ID " +
                "pid: kbasesearchengine.events.exceptions." +
                "UnprocessableEventIndexingException: " +
                "Expected location array for location transform for " +
                "code:1/2/3:subfoo/an id2, got empty array");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.LOCATION_ERROR,
                        "Expected location array for location transform for " +
                                "code:1/2/3:subfoo/an id2, got empty array"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).store(eq(event), eq("LOCATION_ERROR"), argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.LOCATION_ERROR,
                        "Expected location array for location transform for " +
                                "code:1/2/3:subfoo/an id2, got empty array"))));
        
    }
    
    @Test
    public void guidNotFoundError() throws Exception {
        /* tests the handling of missing guids when indexing an object. */
        
        final GUID guid = new GUID("code:1/2/3");
        final GUID dependencyGUID = new GUID("code:4/5/6");
        final SearchObjectType dependentType = new SearchObjectType("Assembly", 1);
        
        final Map<String, Object> data = ImmutableMap.of("assy_ref", dependencyGUID.toString());
        
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
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
                .fromNullableVersion("code", "KBaseGenome.Genome", 3);
        
        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("assy_ref"))
                        .withTransform(Transform.guid(dependentType))
                        .build())
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        when(typeStore.getObjectTypeParsingRules(dependentType)).thenReturn(
                ObjectTypeParsingRules.getBuilder(
                        dependentType,
                        new StorageObjectType("code", "KBaseAssy.Assembly"))
                        .build());
        
        when(ws.buildReferencePaths(Arrays.asList(guid), set(dependencyGUID)))
                .thenReturn(ImmutableMap.of(dependencyGUID, "code:1/2/3;code:4/5/6"));
        
        when(ws.resolveReferences(Arrays.asList(guid), set(dependencyGUID)))
                .thenReturn(set(new ResolvedReference(dependencyGUID, dependencyGUID,
                        new StorageObjectType("code", "Assembly"), Instant.ofEpochMilli(10000))));
        
        when(idxStore.checkParentGuidsExist(set(dependencyGUID)))
                .thenReturn(ImmutableMap.of(dependencyGUID, true));
        
        // i.e. checkParentGuidsExist *does not* imply the indexing records exist
        // because it appears that it doesn't because the parent / access document is written
        // before the data documents, so race conditions can cause a problem
        when(idxStore.getObjectsByIds(set(dependencyGUID))).thenReturn(Collections.emptyList());
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("pid"));
        final StatusEventProcessingState res = worker.processEvent(event);
        assertThat("incorrect state", res, is(StatusEventProcessingState.FAIL));
        
        verify(logger).logError("Error processing event for event NEW_VERSION with parent ID " +
                "pid: kbasesearchengine.events.exceptions." +
                "UnprocessableEventIndexingException: " +
                "GUID code:4/5/6 not found");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.GUID_NOT_FOUND,
                        "GUID code:4/5/6 not found"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).store(eq(event), eq("GUID_NOT_FOUND"), argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.GUID_NOT_FOUND,
                        "GUID code:4/5/6 not found"))));
    }
    
    @Test
    public void skipEvent() throws Exception {
        /* tests the handling of events where no search specifications are available. */
        
        final EventHandler ws = mock(EventHandler.class);
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final IndexingStorage idxStore = mock(IndexingStorage.class);
        final TypeStorage typeStore = mock(TypeStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final Path tempDir = Paths.get(TestCommon.getTempDir()).toAbsolutePath()
                .resolve("IndexerWorkerTest");
        deleteRecursively(tempDir);
        
        when(ws.getStorageCode()).thenReturn("code");
        
        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "KBaseGenome.Genome", 3);
        
        final IndexerWorker worker = new IndexerWorker(
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set());
        
        final StatusEventProcessingState res = worker.processEvent(
                new ChildStatusEvent(StatusEvent.getBuilder(
                        storageObjectType, Instant.ofEpochMilli(10000),
                                StatusEventType.NEW_VERSION)
                        .withNullableAccessGroupID(1)
                        .withNullableObjectID("2")
                        .withNullableVersion(3)
                        .withNullableisPublic(false)
                        .build(),
                        new StatusEventID("pid")));
        assertThat("incorrect state", res, is(StatusEventProcessingState.UNINDX));
        
        verify(logger).logInfo(
                "[Indexer] skipping NEW_VERSION, code:KBaseGenome.Genome-3, code:1/2/3");
    }

    @Test
    public void getEventFromStorageFail() throws Exception {
        
        //TODO TEST allow configuring the retry time to speed this test up - this one's really slow
        
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        when(storage.setAndGetProcessingState(StatusEventProcessingState.READY, null,
                StatusEventProcessingState.PROC, "myid"))
                .thenThrow(new FatalRetriableIndexingException(ErrorType.OTHER, "bonk"));
        
        try {
            worker.runCycle();
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(
                    got, new FatalIndexingException(ErrorType.OTHER, "bonk"));
        }
        
        final String errmsg = "Retriable error in indexer, retry %s: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: bonk";
        
        verify(logger).logError(String.format(errmsg, 1));
        verify(logger).logError(String.format(errmsg, 2));
        verify(logger).logError(String.format(errmsg, 3));
        verify(logger).logError(String.format(errmsg, 4));
        verify(logger).logError(String.format(errmsg, 5));
    }
    
    @Test
    public void noEventsInStorage() throws Exception {
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        when(storage.setAndGetProcessingState(StatusEventProcessingState.READY, null,
                StatusEventProcessingState.PROC, "myid"))
                .thenReturn(Optional.absent());
        
        final boolean res = worker.runCycle();
        assertThat("incorrect result", res, is(false));
    }
    
    @Test
    public void getEventHandlerFail() throws Exception {
        final EventHandler ws = mock(EventHandler.class);
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final IndexingStorage idxStore = mock(IndexingStorage.class);
        final TypeStorage typeStore = mock(TypeStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final Path tempDir = Paths.get(TestCommon.getTempDir()).toAbsolutePath()
                .resolve("IndexerWorkerTest");
        deleteRecursively(tempDir);
        
        when(ws.getStorageCode()).thenReturn("code1");
        
        final IndexerWorker worker = new IndexerWorker(
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        when(storage.setAndGetProcessingState(StatusEventProcessingState.READY, null,
                StatusEventProcessingState.PROC, "myid"))
                .thenReturn(Optional.of(StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                        "CODE", Instant.ofEpochMilli(10000L), StatusEventType.COPY_ACCESS_GROUP)
                        .withNullableAccessGroupID(1)
                        .build(),
                        new StatusEventID("an id"), StatusEventProcessingState.PROC)
                        .build()));
        
        final boolean res = worker.runCycle();
        assertThat("incorrect result", res, is(true));
        
        verify(logger).logError("Error getting event handler for event COPY_ACCESS_GROUP " +
                "an id: kbasesearchengine.events.exceptions." +
                "UnprocessableEventIndexingException: " +
                "No event handler for storage code CODE is registered");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new UnprocessableEventIndexingException(ErrorType.OTHER,
                        "No event handler for storage code CODE is registered"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).setProcessingState(
                eq(new StatusEventID("an id")),
                eq(StatusEventProcessingState.PROC),
                eq("OTHER"),
                argThat(new ThrowableMatcher(new UnprocessableEventIndexingException(
                        ErrorType.OTHER,
                        "No event handler for storage code CODE is registered"))));
    }
    
    @Test
    public void handleFatalException() throws Exception {
        // tests that a fatal exception is rethrown and not just logged / stored.
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "sometype", 3);

        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final GUID guid = new GUID("code:1/2/3");
        when(idxStore.checkParentGuidsExist(set(guid))).thenReturn(ImmutableMap.of(guid, false));
        
        when(ws.load(eq(Arrays.asList(guid)), any(Path.class)))
                .thenThrow(new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo"));

        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,
                Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("parentID"));
        try {
            worker.processEvent(event);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got,
                    new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo"));
        }
        
        verify(logger).logError("Error processing event for event NEW_VERSION with parent ID " +
                "parentID: kbasesearchengine.events.exceptions." +
                "FatalIndexingException: " +
                "WS is super broke yo");
        
        verify(logger).logError(argThat(new ThrowableMatcher(
                new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo"))));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
        
        verify(storage).store(eq(event), eq("OTHER"), argThat(new ThrowableMatcher(
                new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo"))));
    }
    
    @Test
    public void storeErrorFail() throws Exception {
        
        //TODO TEST allow configuring the retry time to speed this test up - this one's really slow
        
        // tests handling the case where an attempt to store an error fails.
        // at this point the code should just bail, something is very wrong.
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
                "myid", Arrays.asList(ws), storage, idxStore, typeStore, tempDir.toFile(), logger,
                null, 1000);
        
        final StorageObjectType storageObjectType = StorageObjectType
                .fromNullableVersion("code", "sometype", 3);

        final ObjectTypeParsingRules rule = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), storageObjectType)
                .build();
        when(typeStore.listObjectTypeParsingRules(storageObjectType)).thenReturn(set(rule));
        
        final GUID guid = new GUID("code:1/2/3");
        when(idxStore.checkParentGuidsExist(set(guid))).thenReturn(ImmutableMap.of(guid, false));
        
        when(ws.load(eq(Arrays.asList(guid)), any(Path.class)))
                .thenThrow(new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo"));
        
        final ChildStatusEvent event = new ChildStatusEvent(StatusEvent.getBuilder(
                storageObjectType,
                Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("2")
                .withNullableVersion(3)
                .withNullableisPublic(false)
                .build(),
                new StatusEventID("parentID"));
        
        when(storage.store(eq(event), eq("OTHER"), argThat(new ThrowableMatcher(
                new FatalIndexingException(ErrorType.OTHER, "WS is super broke yo")))))
                .thenThrow(new FatalRetriableIndexingException(
                        ErrorType.OTHER, "Storage is also super broke yo"));
        
        try {
            worker.processEvent(event);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got,
                    new FatalIndexingException(ErrorType.OTHER, "Storage is also super broke yo"));
        }
        
        final String errmsg = "Retriable error in indexer for event NEW_VERSION with parent ID " +
                "parentID, retry %s: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: " +
                "Storage is also super broke yo";
        
        verify(logger).logError(String.format(errmsg, 1));
        verify(logger).logError(String.format(errmsg, 2));
        verify(logger).logError(String.format(errmsg, 3));
        verify(logger).logError(String.format(errmsg, 4));
        verify(logger).logError(String.format(errmsg, 5));
        
        verify(idxStore, never()).indexObjects(
                any(), any(), any(), any(), any(), any(), anyBoolean());
    }
}
