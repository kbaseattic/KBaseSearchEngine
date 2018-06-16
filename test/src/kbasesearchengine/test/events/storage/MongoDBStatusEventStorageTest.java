package kbasesearchengine.test.events.storage;

import static com.mongodb.client.model.Filters.eq;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static kbasesearchengine.test.common.TestCommon.set;

import java.io.PrintWriter;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Range;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredChildStatusEvent;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.UnprocessableEventIndexingException;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.test.controllers.mongo.MongoController;

public class MongoDBStatusEventStorageTest {
    
    final private static String LONG1001;
    final private static String LONG100_001;
    static {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("01234567890");
        }
        sb.append("a");
        LONG1001 = sb.toString();
        
        final StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < 10_000; i++) {
            sb2.append("012345678\n");
        }
        sb2.append("a");
        
        LONG100_001 = sb2.toString();
    }
    
    @SuppressWarnings("serial")
    final private Exception LONG_TRACE_EXCEPTION = new Exception("foo") {
        
        @Override
        public void printStackTrace(final PrintWriter pw) {
            pw.print(LONG100_001);
        }
    };
    
    

    private StatusEventStorage storage;
    private static MongoController mongo;
    private static MongoDatabase db;
    private static MongoClient mc;
    private Clock clock;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestCommon.stfuLoggers();
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                Paths.get(TestCommon.getTempDir()),
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        db = mc.getDatabase("test_mongostorage");
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        if (mc != null) {
            mc.close();
        }
        if (mongo != null) {
            mongo.destroy(TestCommon.getDeleteTempFiles());
        }
    }
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(db);
        clock = mock(Clock.class);
        storage  = new MongoDBStatusEventStorage(db, clock);
    }

    @Test
    public void storeAndGetNoTypeEmptyCodesAllFields() throws Exception {
        // tests with all possible fields in status event
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build(),
                StatusEventProcessingState.UNPROC,
                set(),
                "Baldrick");
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", sse.getStoredBy(), is(Optional.of("Baldrick")));
        assertThat("incorrect store time", sse.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000L))));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", sse.getErrorStackTrace(), is(Optional.absent()));
        assertNotNull("id is null", sse.getID());
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("Baldrick")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void storeAndGetNoTypeWithCodes() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC,
                set("business", "numbers"),
                "WSEG");
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("business", "numbers")));
        assertThat("incorrect stored by", sse.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", sse.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000L))));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", sse.getErrorStackTrace(), is(Optional.absent()));
        
        assertNotNull("id is null", sse.getID());
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("business", "numbers")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void storeAndGetWithTypeMinimalFields() throws Exception {
        // tests with minimal possible fields in status event
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(40000L));
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS).build(),
                StatusEventProcessingState.FAIL, null, "WSEG");
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                    StatusEventType.DELETE_ALL_VERSIONS)
                .build()));
        assertNotNull("id is null", sse.getID());
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", sse.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", sse.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(40000L))));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", sse.getErrorStackTrace(), is(Optional.absent()));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(40000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void storeAndGetNonExistant() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000L));
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS).build(),
                StatusEventProcessingState.FAIL, null, "WS");
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                    StatusEventType.DELETE_ALL_VERSIONS)
                .build()));
        assertNotNull("id is null", sse.getID());
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", sse.getStoredBy(), is(Optional.of("WS")));
        assertThat("incorrect store time", sse.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(100000L))));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", sse.getErrorStackTrace(), is(Optional.absent()));
        
        final Optional<StoredStatusEvent> got = storage.get(
                new StatusEventID(new ObjectId().toString()));
        assertThat("expected absent", got, is(Optional.absent()));
    }
    
    @Test
    public void getWithMissingCodesField() throws Exception {
        getWithAlteredCodesField(new Document("$unset", new Document("wrkcde", 1)));
    }
    
    @Test
    public void getWithNullCodesField() throws Exception {
        getWithAlteredCodesField(new Document("$set", new Document("wrkcde", null)));
    }
    
    @Test
    public void getWithEmptyCodesField() throws Exception {
        getWithAlteredCodesField(new Document("$set", new Document("wrkcde", Arrays.asList())));
    }

    private void getWithAlteredCodesField(final Document operation)
            throws FatalRetriableIndexingException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC,
                set("business", "numbers"), "WSEG");
        
        db.getCollection("searchEvents").findOneAndUpdate(
                new Document("_id", new ObjectId(sse.getID().getId())),
                operation,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void getWithNullStoredByAndTime() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC,
                null, "WSEG");
        
        db.getCollection("searchEvents").findOneAndUpdate(
                new Document("_id", new ObjectId(sse.getID().getId())),
                new Document("$set", new Document("stby", null).append("sttime", null)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect store time", got.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void storeAndGetChildNoTypeAllFields() throws Exception {
        // tests with all possible fields in status event
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredChildStatusEvent sse = storage.store(new ChildStatusEvent(
                StatusEvent.getBuilder(
                    "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                    .withNullableAccessGroupID(6)
                    .withNullableisPublic(true)
                    .withNullableNewName("foo")
                    .withNullableObjectID("bar")
                    .withNullableVersion(7)
                    .build(),
                    new StatusEventID("parent id")),
                "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect event", sse.getChildEvent(), is(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                        .withNullableAccessGroupID(6)
                        .withNullableisPublic(true)
                        .withNullableNewName("foo")
                        .withNullableObjectID("bar")
                        .withNullableVersion(7)
                        .build(),
                new StatusEventID("parent id"))));
        assertThat("incorrect store time", sse.getStoreTime(), is(Instant.ofEpochMilli(30000L)));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                sse.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
        assertNotNull("id is null", sse.getID());
        
        final StoredChildStatusEvent got = storage.getChild(sse.getID()).get();
        
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect event", got.getChildEvent(), is(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                        .withNullableAccessGroupID(6)
                        .withNullableisPublic(true)
                        .withNullableNewName("foo")
                        .withNullableObjectID("bar")
                        .withNullableVersion(7)
                        .build(),
                new StatusEventID("parent id"))));
        assertThat("incorrect store time", got.getStoreTime(), is(Instant.ofEpochMilli(30000L)));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                got.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
    }
    
    @Test
    public void storeAndGetChildWithTypeMinimalFields() throws Exception {
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredChildStatusEvent sse = storage.store(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        sot, Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                                .build(),
                        new StatusEventID("parent id")),
                "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect event", sse.getChildEvent(), is(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        sot, Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                                .build(),
                        new StatusEventID("parent id"))));
        assertThat("incorrect store time", sse.getStoreTime(), is(Instant.ofEpochMilli(30000L)));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                sse.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
        assertNotNull("id is null", sse.getID());
        
        final StoredChildStatusEvent got = storage.getChild(sse.getID()).get();
        
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect event", got.getChildEvent(), is(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        sot, Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                                .build(),
                        new StatusEventID("parent id"))));
        assertThat("incorrect store time", got.getStoreTime(), is(Instant.ofEpochMilli(30000L)));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                got.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
    }
    
    @Test
    public void storeAndGetChildWithErrMsgTruncation() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredChildStatusEvent sse = storage.store(new ChildStatusEvent(
                StatusEvent.getBuilder(
                    "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                    .build(),
                    new StatusEventID("parent id")),
                "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, LONG1001));

        final String msgExpected = LONG1001.substring(0, 997) + "...";
        
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.of(msgExpected)));
        
        final StoredChildStatusEvent got = storage.getChild(sse.getID()).get();
        
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of(msgExpected)));
    }
    
    @Test
    public void storeAndGetChildWithTraceTruncation() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        final StoredChildStatusEvent sse = storage.store(new ChildStatusEvent(
                StatusEvent.getBuilder(
                    "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                    .build(),
                    new StatusEventID("parent id")),
                "DELETED",
                LONG_TRACE_EXCEPTION);

        final String traceExpected = LONG100_001.substring(0, 99997) + "...";
        
        assertThat("incorrect error msg", sse.getErrorStackTrace(),
                is(Optional.of(traceExpected)));
        
        final StoredChildStatusEvent got = storage.getChild(sse.getID()).get();
        
        assertThat("incorrect error msg", got.getErrorStackTrace(),
                is(Optional.of(traceExpected)));
    }
    
    @Test
    public void storeAndGetChildNonExistant() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000L));
        final StoredChildStatusEvent sse = storage.store(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        "WS", Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS)
                                .build(),
                new StatusEventID("parent id")),
                "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect event", sse.getChildEvent(), is(new ChildStatusEvent(
                StatusEvent.getBuilder(
                        "WS", Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS)
                                .build(),
                new StatusEventID("parent id"))));
        assertNotNull("id is null", sse.getID());
        assertThat("incorrect store time", sse.getStoreTime(), is(Instant.ofEpochMilli(100000L)));
        assertThat("incorrect error code", sse.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", sse.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                sse.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
        
        final Optional<StoredChildStatusEvent> got = storage.getChild(
                new StatusEventID(new ObjectId().toString()));
        assertThat("expected absent", got, is(Optional.absent()));
    }
    
    @Test
    public void setProcessingStateWithoutOldState() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(10000), Instant.ofEpochMilli(60000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        
        final boolean success = storage.setProcessingState(
                sse.getID(), null, StatusEventProcessingState.FAIL);
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(60000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(10000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void setProcessingStateWithOldState() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(40000), Instant.ofEpochMilli(10000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        final boolean success = storage.setProcessingState(
                sse.getID(), StatusEventProcessingState.UNPROC, StatusEventProcessingState.FAIL);
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(10000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(40000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.absent()));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.absent()));
        assertThat("incorrect error trace", got.getErrorStackTrace(), is(Optional.absent()));
    }
    
    @Test
    public void setProcessingStateWithErrorWithoutOldState() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(10000), Instant.ofEpochMilli(60000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        final boolean success = storage.setProcessingState(
                sse.getID(), null, "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(60000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(10000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                got.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
    }
    
    @Test
    public void setProcessingStateWithErrorWithOldState() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(40000), Instant.ofEpochMilli(10000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        final boolean success = storage.setProcessingState(
                sse.getID(), StatusEventProcessingState.UNPROC, "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(10000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(40000L))));
        assertThat("incorrect error code", got.getErrorCode(), is(Optional.of("DELETED")));
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of("deleted")));
        assertThat("incorrect error trace",
                got.getErrorStackTrace().get().contains("IndexingException: deleted"), is(true));
    }
    
    @Test
    public void setProcessingStateWithErrorWithErrMsgTruncation() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(40000), Instant.ofEpochMilli(10000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        final boolean success = storage.setProcessingState(
                sse.getID(), StatusEventProcessingState.UNPROC, "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, LONG1001));
        assertThat("expected success", success, is(true));
        
        final String msgExpected = LONG1001.substring(0, 997) + "...";
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        
        assertThat("incorrect error msg", got.getErrorMessage(), is(Optional.of(msgExpected)));
    }
    
    @Test
    public void setProcessingStateWithErrorWithErrTraceTruncation() throws Exception {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(40000), Instant.ofEpochMilli(10000));
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WSEG");
        
        final boolean success = storage.setProcessingState(
                sse.getID(), StatusEventProcessingState.UNPROC, "DELETED",
                LONG_TRACE_EXCEPTION);
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();

        final String traceExpected = LONG100_001.substring(0, 99997) + "...";
        
        assertThat("incorrect error msg", got.getErrorStackTrace(),
                is(Optional.of(traceExpected)));
    }
    
    @Test
    public void setProcessingStateFailNonExistant() throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        storage.store(StatusEvent.getBuilder(
                "FE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WS");
        
        
        final boolean success = storage.setProcessingState(
                new StatusEventID(new ObjectId().toString()), null,
                StatusEventProcessingState.FAIL);
        
        assertThat("expected fail", success, is(false));
        
        final boolean success2 = storage.setProcessingState(
                new StatusEventID(new ObjectId().toString()), null, "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        
        assertThat("expected fail", success2, is(false));
    }
    
    @Test
    public void setProcessingStateFailNoSuchState() throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "FE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null, "WS");
        
        final boolean success = storage.setProcessingState(sse.getID(),
                StatusEventProcessingState.READY, StatusEventProcessingState.FAIL);
        
        assertThat("expected fail", success, is(false));
        
        final boolean success2 = storage.setProcessingState(sse.getID(),
                StatusEventProcessingState.READY, "DELETED",
                new UnprocessableEventIndexingException(ErrorType.DELETED, "deleted"));
        
        assertThat("expected fail", success2, is(false));
    }

    @Test
    public void getAndSetProcessingWithSortNoDBWorkerCodeField() throws Exception {
        getAndSetProcessingWithSort(set(),
                id -> db.getCollection("searchEvents").findOneAndUpdate(
                        new Document("_id", new ObjectId(id.getId())),
                        new Document("$unset", new Document("wrkcde", 1)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }
    
    @Test
    public void getAndSetProcessingWithSortNullDBWorkerCodeField() throws Exception {
        getAndSetProcessingWithSort(null,
                id -> db.getCollection("searchEvents").findOneAndUpdate(
                        new Document("_id", new ObjectId(id.getId())),
                        new Document("$set", new Document("wrkcde", null)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }
    
    @Test
    public void getAndSetProcessingWithSortEmptyDBWorkerCodeField() throws Exception {
        getAndSetProcessingWithSort(set("default", "bar"),
                id -> db.getCollection("searchEvents").findOneAndUpdate(
                        new Document("_id", new ObjectId(id.getId())),
                        new Document("$set", new Document("wrkcde", Arrays.asList())),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }
    
    private static final Consumer<StatusEventID> NOOP = id -> {};
    
    @Test
    public void getAndSetProcessingWithSortAndNullCodes() throws Exception {
        getAndSetProcessingWithSort(null, NOOP);
    }
    
    @Test
    public void getAndSetProcessingWithSortAndEmptyCodes() throws Exception {
        getAndSetProcessingWithSort(set(), NOOP);
    }
    
    @Test
    public void getAndSetProcessingWithSortAndDefaultCode() throws Exception {
        getAndSetProcessingWithSort(set("default"), NOOP);
    }
    
    @Test
    public void getAndSetProcessingWithSortAndDefaultPlusCodes() throws Exception {
        getAndSetProcessingWithSort(set("default", "foo"), NOOP);
    }
    
    @Test
    public void getAndSetProcessingWithSortAndSpecificCodes() throws Exception {
        getAndSetProcessingWithSort(set("bar"), set("baz"), set("bar"));
    }
    
    @Test
    public void getAndSetProcessingWithSortAndSpecificOverlappingCodes() throws Exception {
        getAndSetProcessingWithSort(set("foo", "bar"), set("foo", "baz"), set("bar"));
    }
    
    @Test
    public void getAndSetProcessingWithSortAndSameCodes() throws Exception {
        // tests sort
        getAndSetProcessingWithSort(set("foo", "baz"), set("foo", "baz"), set("baz"));
    }

    private void getAndSetProcessingWithSort(
            final Set<String> searchWorkerCodes,
            final Consumer<StatusEventID> operation)
            throws RetriableIndexingException, FatalRetriableIndexingException {
        getAndSetProcessingWithSort(null, set("default"), null, searchWorkerCodes, operation);
    }
    
    private void getAndSetProcessingWithSort(
            final Set<String> storedWorkerCodes,
            final Set<String> otherEventWorkerCodes,
            final Set<String> searchWorkerCodes)
            throws FatalRetriableIndexingException, RetriableIndexingException {
        getAndSetProcessingWithSort(storedWorkerCodes, storedWorkerCodes, otherEventWorkerCodes,
                searchWorkerCodes, NOOP);
    }
    
    private void getAndSetProcessingWithSort(
            final Set<String> storedWorkerCodes,
            final Set<String> expectedWorkerCodes,
            final Set<String> otherEventWorkerCodes,
            final Set<String> searchWorkerCodes,
            final Consumer<StatusEventID> operation)
            throws RetriableIndexingException, FatalRetriableIndexingException {
        // test that the event that is updated is the oldest event
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000));
        store(2, 200, StatusEventProcessingState.READY, otherEventWorkerCodes);
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(1000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build(),
                StatusEventProcessingState.READY,
                storedWorkerCodes,
                "WSEG");
        store(201, 400, StatusEventProcessingState.READY, otherEventWorkerCodes);
        
        operation.accept(sse.getID());
        
        final StoredStatusEvent ret = storage.setAndGetProcessingState(
                StatusEventProcessingState.READY, searchWorkerCodes,
                StatusEventProcessingState.PROC, "whee")
                .get();
        assertThat("incorrect state", ret.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", ret.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", ret.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", ret.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(1000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", ret.getID(), is(sse.getID()));
        assertThat("incorrect worker codes", ret.getWorkerCodes(), is(expectedWorkerCodes));
        assertThat("incorrect stored by", ret.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", ret.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(100000L))));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(1000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(expectedWorkerCodes));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(100000L))));
    }
    
    @Test
    public void getAndSetProcessingSortWithCodes() throws Exception {
        // tests that earlier events with different codes will be skipped
        // and that the newest event with matching codes is returned
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000));
        store(1, 10, StatusEventProcessingState.READY, set("foo"));
        store(12, 20, StatusEventProcessingState.READY, set("bar"));
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(11000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build(),
                StatusEventProcessingState.READY,
                set("bar"),
                "WSEG");
        
        
        final StoredStatusEvent ret = storage.setAndGetProcessingState(
                StatusEventProcessingState.READY, set("bar"),
                StatusEventProcessingState.PROC, "whee")
                .get();
        assertThat("incorrect state", ret.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", ret.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", ret.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", ret.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(11000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", ret.getID(), is(sse.getID()));
        assertThat("incorrect worker codes", ret.getWorkerCodes(), is(set("bar")));
        assertThat("incorrect stored by", ret.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", ret.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(100000L))));
        
        final StoredStatusEvent got = storage.get(sse.getID()).get();
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(11000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", got.getID(), is(sse.getID()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("bar")));
        assertThat("incorrect stored by", got.getStoredBy(), is(Optional.of("WSEG")));
        assertThat("incorrect store time", got.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(100000L))));
    }
    
    @Test
    public void getAndSetProcessingNonExistantOnState() throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build(),
                StatusEventProcessingState.UNPROC,
                null,
                "WS");
        
        final Optional<StoredStatusEvent> ret = storage.setAndGetProcessingState(
                StatusEventProcessingState.READY, null, StatusEventProcessingState.PROC, "whee");
        assertThat("expected absent", ret, is(Optional.absent()));
    }
    
    @Test
    public void getAndSetProcessingNonExistantOnWorkerCode() throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        store(1, 2, StatusEventProcessingState.READY, set("foo"));
        store(1, 2, StatusEventProcessingState.READY, set("bar"));
        
        when(clock.instant()).thenReturn(Instant.now());
        
        final Optional<StoredStatusEvent> ret = storage.setAndGetProcessingState(
                StatusEventProcessingState.READY, set("baz"),
                StatusEventProcessingState.PROC, "whee");
        assertThat("expected absent", ret, is(Optional.absent()));
    }
    
    @Test
    public void setAndGetProcessingFail() {
        failSetAndGetProcessing(null, null, StatusEventProcessingState.FAIL, "foo",
                new NullPointerException("oldState"));
        failSetAndGetProcessing(StatusEventProcessingState.FAIL, null, null, "foo",
                new NullPointerException("newState"));
        failSetAndGetProcessing(StatusEventProcessingState.UNPROC, null,
                StatusEventProcessingState.READY, null,
                new IllegalArgumentException("updater cannot be null or whitespace"));
        failSetAndGetProcessing(StatusEventProcessingState.UNPROC, null,
                StatusEventProcessingState.READY, "   \t \n  ",
                new IllegalArgumentException("updater cannot be null or whitespace"));
        failSetAndGetProcessing(StatusEventProcessingState.UNPROC, set("foo", null),
                StatusEventProcessingState.FAIL, "foo",
                new NullPointerException("null item in workerCodes"));
    }
    
    private void failSetAndGetProcessing(
            final StatusEventProcessingState oldState,
            final Set<String> workerCodes,
            final StatusEventProcessingState newState,
            final String updater,
            final Exception expected) {
        try {
            storage.setAndGetProcessingState(oldState, workerCodes, newState, updater);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void getByState() throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        store(10100, StatusEventProcessingState.UNPROC);
        store(10, StatusEventProcessingState.UNINDX);
        store(10, StatusEventProcessingState.FAIL);
        
        //basic checks
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 15, 15, Range.closed(1, 15));
        assertReturnedInOrder(StatusEventProcessingState.UNINDX, 15, 10, Range.closed(1, 10));
        assertReturnedInOrder(StatusEventProcessingState.FAIL, 15, 10, Range.closed(1, 10));
        // ensures no events are found for INDX
        assertReturnedInOrder(StatusEventProcessingState.INDX, 15, 0, Range.closed(-1, -1));
        
        // check limit works as expected
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 0, 10000, Range.closed(1, 10000));
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 1, 1, Range.closed(1, 1));
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 9999, 9999,
                Range.closed(1, 9999));
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 10000, 10000,
                Range.closed(1, 10000));
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, 10001, 10000,
                Range.closed(1, 10000));
        
        // check changing state moves limits around
        setState(StatusEventProcessingState.UNPROC, StatusEventProcessingState.INDX,
                Range.closed(100, 150));
        
        assertReturnedInOrder(StatusEventProcessingState.INDX, 25, 25, Range.closed(100, 124));
        assertReturnedInOrder(StatusEventProcessingState.INDX, 51, 51, Range.closed(100, 150));
        assertReturnedInOrder(StatusEventProcessingState.INDX, 52, 51, Range.closed(100, 150));
        assertReturnedInOrder(StatusEventProcessingState.INDX, -1, 51, Range.closed(100, 150));
        assertReturnedInOrder(StatusEventProcessingState.UNPROC, -1, 10000,
                Range.closed(1, 99), Range.closed(151, 10051));
    }

    private void setState(
            final StatusEventProcessingState oldState,
            final StatusEventProcessingState newState,
            final Range<Integer> timesToModifyInSec)
            throws Exception {
        when(clock.instant()).thenReturn(Instant.now());
        for (final StoredStatusEvent event: storage.get(oldState, -1)) {
            final int t = (int) event.getEvent().getTimestamp().toEpochMilli();
            if (timesToModifyInSec.contains(t / 1000)) {
                storage.setProcessingState(event.getID(), oldState, newState);
            }
        }
    }

    @SafeVarargs
    private final void assertReturnedInOrder(
            final StatusEventProcessingState state,
            final int limit,
            final int expected,
            final Range<Integer>... ranges)
            throws Exception {
        Instant last = null;
        final List<StoredStatusEvent> events = storage.get(state, limit);
        assertThat("incorrect number of events", events.size(), is(expected));
        for (final StoredStatusEvent event: events) {
            if (last != null) {
                assertThat("expected correct ordering",
                        last.isBefore(event.getEvent().getTimestamp()), is(true));
            }
            last = event.getEvent().getTimestamp();
            assertInRange(event.getEvent().getTimestamp(), ranges);
        }
    }

    private void assertInRange(final Instant timestamp, final Range<Integer>[] ranges) {
        final int t = (int) timestamp.toEpochMilli();
        for (final Range<Integer> range: ranges) {
            if (range.contains(t / 1000)) {
                return;
            }
        }
        fail(String.format("Time %s not in any ranges %s", t / 1000, Arrays.asList(ranges)));
    }

    private void store(final int count, final StatusEventProcessingState state)
            throws RetriableIndexingException {
        store(1, count, state, null);
    }
    
    private void store(
            final int start,
            final int count,
            final StatusEventProcessingState state,
            final Set<String> workerCodes)
            throws RetriableIndexingException {
        final List<Integer> times = new ArrayList<>();
        for (int i = start; i <= count; i++) {
            times.add(i);
        }
        Collections.shuffle(times);
        for (final int time: times) {
            storage.store(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(time * 1000), StatusEventType.NEW_VERSION).build(),
                    state, workerCodes, "WSEG");
        }
    }
    
    @Test
    public void constructFail() {
        try {
            new MongoDBStatusEventStorage(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("db"));
        }
    }
    
    @Test
    public void storeFail() {
        failStore(null, StatusEventProcessingState.UNINDX, null, "s",
                new NullPointerException("newEvent"));
        final StatusEvent event = StatusEvent.getBuilder(
                "Ws", Instant.ofEpochMilli(10000), StatusEventType.NEW_ALL_VERSIONS).build();
        failStore(event, null, null, "s", new NullPointerException("state"));
        failStore(event, StatusEventProcessingState.UNINDX, set("foo", null), "s",
                new IllegalArgumentException("null or whitespace only item in workerCodes"));
        failStore(event, StatusEventProcessingState.UNINDX, set("foo", "   \t   \n  "), "s",
                new IllegalArgumentException("null or whitespace only item in workerCodes"));
        failStore(event, StatusEventProcessingState.UNINDX, set(), null,
                new IllegalArgumentException("storedBy cannot be null or whitespace only"));
        failStore(event, StatusEventProcessingState.UNINDX, set(), "   \t   \n  ",
                new IllegalArgumentException("storedBy cannot be null or whitespace only"));
    }
    
    private void failStore(
            final StatusEvent event,
            final StatusEventProcessingState state,
            final Set<String> workerCodes,
            final String storedBy,
            final Exception expected) {
        try {
            storage.store(event, state, workerCodes, storedBy);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void storeChildFail() {
        final ChildStatusEvent c = new ChildStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS).build(),
                new StatusEventID("parent id"));
        final Throwable e = new RuntimeException("foo");
        
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(10000L));
        
        failStoreChild(null, "c", e, new NullPointerException("newEvent"));
        
        failStoreChild(c, null, e, new IllegalArgumentException(
                "errorCode cannot be null or whitespace only"));
        failStoreChild(c, "   \t   \n   ", e, new IllegalArgumentException(
                "errorCode cannot be null or whitespace only"));
        failStoreChild(c, "01234567890123456789a", e, new IllegalArgumentException(
                "errorCode exceeds max length of 20"));
        
        failStoreChild(c, "c", null, new NullPointerException("error"));
    }
    
    private void failStoreChild(
            final ChildStatusEvent event,
            final String errorCode,
            final Throwable error,
            final Exception expected) {
        try {
            storage.store(event, errorCode, error);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void getFail() {
        try {
            storage.get(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("id"));
        }
    }
    
    @Test
    public void getChildFail() {
        try {
            storage.getChild(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("id"));
        }
    }
    
    @Test
    public void getByStateFail() {
        try {
            storage.get(null, -1);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("state"));
        }
    }
    
    @Test
    public void setProcessingStateFail() {
        failSetProcessingState(
                null, StatusEventProcessingState.INDX, new NullPointerException("id"));
        failSetProcessingState(
                new StatusEventID("foo"), null, new NullPointerException("newState"));
    }
    
    private void failSetProcessingState(
            final StatusEventID id,
            final StatusEventProcessingState state,
            final Exception expected) {
        try {
            storage.setProcessingState(id, null, state);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void setProcessingStateWithErrorFail() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000));
        
        final StatusEventID i = new StatusEventID("foo");
        failSetProcessingState(null, "c", LONG_TRACE_EXCEPTION, new NullPointerException("id"));
        failSetProcessingState(i, null, LONG_TRACE_EXCEPTION,
                new IllegalArgumentException("errorCode cannot be null or whitespace only"));
        failSetProcessingState(i, "   \t \n  ", LONG_TRACE_EXCEPTION,
                new IllegalArgumentException("errorCode cannot be null or whitespace only"));
        failSetProcessingState(i, "01234567890123456789a", LONG_TRACE_EXCEPTION,
                new IllegalArgumentException("errorCode exceeds max length of 20"));
        failSetProcessingState(i, "c", null, new NullPointerException("error"));
    }
    
    private void failSetProcessingState(
            final StatusEventID id,
            final String errorCode,
            final Throwable error,
            final Exception expected) {
        try {
            storage.setProcessingState(id, null, errorCode, error);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void searchEventsIndexes() {
        final Set<Document> indexes = new HashSet<>();
        // this is annoying. MongoIterator has two forEach methods with different signatures
        // and so which one to call is ambiguous for lambda expressions.
        db.getCollection("searchEvents").listIndexes().forEach((Consumer<Document>) indexes::add);
        for (final Document d: indexes) {
            d.remove("v"); // remove the mongo index version which is no business of ours
        }
        assertThat("incorrect indexes", indexes, is(set(
                new Document()
                        .append("key", new Document("status", 1).append("time", 1))
                        .append("name", "status_1_time_1")
                        .append("ns", "test_mongostorage.searchEvents"),
                new Document()
                        .append("key", new Document("sttime", 1).append("status", 1))
                        .append("name", "sttime_1_status_1")
                        .append("ns", "test_mongostorage.searchEvents"),
                new Document()
                        .append("key", new Document("_id", 1))
                        .append("name", "_id_")
                        .append("ns", "test_mongostorage.searchEvents")
                )));
    }

    @Test
    public void resetFailedEvents()
                  throws RetriableIndexingException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000L));
        store(10, StatusEventProcessingState.FAIL);
        store(2, StatusEventProcessingState.READY);
        store(2, StatusEventProcessingState.INDX);
        store(2, StatusEventProcessingState.PROC);
        store(2, StatusEventProcessingState.UNINDX);
        store(2, StatusEventProcessingState.UNPROC);

        long numObjects = db.getCollection("searchEvents").count();
        assertThat("incorrect number of events", numObjects, is(20L));
        assertStateCount(StatusEventProcessingState.FAIL, 10L);
        assertStateCount(StatusEventProcessingState.UNPROC, 2L);
        assertStateCount(StatusEventProcessingState.UNINDX, 2L);
        assertStateCount(StatusEventProcessingState.PROC, 2L);
        assertStateCount(StatusEventProcessingState.INDX, 2L);
        assertStateCount(StatusEventProcessingState.READY, 2L);

        storage.resetFailedEvents();

        numObjects = db.getCollection("searchEvents").count();
        assertThat("incorrect number of events", numObjects, is(20L));
        assertStateCount(StatusEventProcessingState.FAIL, 0L);
        assertStateCount(StatusEventProcessingState.UNPROC, 12L);  // fail + unproc
        assertStateCount(StatusEventProcessingState.UNINDX, 2L);
        assertStateCount(StatusEventProcessingState.PROC, 2L);
        assertStateCount(StatusEventProcessingState.INDX, 2L);
        assertStateCount(StatusEventProcessingState.READY, 2L);
    }

    private void assertStateCount(StatusEventProcessingState state, long count) {

        long numObjects = db.getCollection("searchEvents").
                count(eq("status", state.toString()));
        assertThat("incorrect number of events", numObjects, is(count));
    }

    @Test
    public void childEventsIndexes() {
        final Set<Document> indexes = new HashSet<>();
        // this is annoying. MongoIterator has two forEach methods with different signatures
        // and so which one to call is ambiguous for lambda expressions.
        db.getCollection("childEvents").listIndexes().forEach((Consumer<Document>) indexes::add);
        for (final Document d: indexes) {
            d.remove("v"); // remove the mongo index version which is no business of ours
        }
        assertThat("incorrect indexes", indexes, is(set(
                new Document()
                        .append("key", new Document("sttime", 1).append("status", 1))
                        .append("name", "sttime_1_status_1")
                        .append("ns", "test_mongostorage.childEvents"),
                new Document()
                        .append("key", new Document("_id", 1))
                        .append("name", "_id_")
                        .append("ns", "test_mongostorage.childEvents")
                )));
    }
}
