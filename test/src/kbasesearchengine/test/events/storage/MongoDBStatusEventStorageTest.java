package kbasesearchengine.test.events.storage;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static kbasesearchengine.test.common.TestCommon.set;

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

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.test.controllers.mongo.MongoController;

public class MongoDBStatusEventStorageTest {

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
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build(),
                StatusEventProcessingState.UNPROC,
                set());
        
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
        assertNotNull("id is null", sse.getId());
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
    }
    
    @Test
    public void storeAndGetNoTypeWithCodes() throws Exception {
        // tests with all possible fields in status event
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC,
                set("business", "numbers"));
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("business", "numbers")));
        assertNotNull("id is null", sse.getId());
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("business", "numbers")));
    }
    
    @Test
    public void storeAndGetWithTypeMinimalFields() throws Exception {
        // tests with minimal possible fields in statusevent
        
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS).build(),
                StatusEventProcessingState.FAIL, null);
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                    StatusEventType.DELETE_ALL_VERSIONS)
                .build()));
        assertNotNull("id is null", sse.getId());
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
    }
    
    @Test
    public void storeAndGetNonExistant() throws Exception {
        
        final StorageObjectType sot = new StorageObjectType("foo", "bar", 9);
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(20000), StatusEventType.DELETE_ALL_VERSIONS).build(),
                StatusEventProcessingState.FAIL, null);
        
        assertThat("incorrect state", sse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", sse.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sse.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", sse.getEvent(), is(StatusEvent.getBuilder(
                new StorageObjectType("foo", "bar", 9), Instant.ofEpochMilli(20000),
                    StatusEventType.DELETE_ALL_VERSIONS)
                .build()));
        assertNotNull("id is null", sse.getId());
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
        
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
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC,
                set("business", "numbers"));
        
        db.getCollection("searchEvents").findOneAndUpdate(
                new Document("_id", new ObjectId(sse.getId().getId())),
                operation,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", got.getWorkerCodes(), is(set("default")));
    }
    
    @Test
    public void setProcessingStateWithoutOldState() throws Exception {
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null);
        
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(30000));
        
        final boolean success = storage.setProcessingState(
                sse.getId(), null, StatusEventProcessingState.FAIL);
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(30000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
    }
    
    @Test
    public void setProcessingStateWithOldState() throws Exception {
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null);
        
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(10000));
        
        final boolean success = storage.setProcessingState(
                sse.getId(), StatusEventProcessingState.UNPROC, StatusEventProcessingState.FAIL);
        assertThat("expected success", success, is(true));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", got.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(10000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                "KE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("default")));
    }
    
    @Test
    public void setProcessingStateFailNonExistant() throws Exception {
        storage.store(StatusEvent.getBuilder(
                "FE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null);
        
        when(clock.instant()).thenReturn(Instant.now());
        
        final boolean success = storage.setProcessingState(
                new StatusEventID(new ObjectId().toString()), null,
                StatusEventProcessingState.FAIL);
        
        assertThat("expected fail", success, is(false));
    }
    
    @Test
    public void setProcessingStateFailNoSuchState() throws Exception {
        final StoredStatusEvent sse = storage.store(StatusEvent.getBuilder(
                "FE", Instant.ofEpochMilli(30000), StatusEventType.COPY_ACCESS_GROUP).build(),
                StatusEventProcessingState.UNPROC, null);
        
        when(clock.instant()).thenReturn(Instant.now());
        
        final boolean success = storage.setProcessingState(sse.getId(),
                StatusEventProcessingState.READY, StatusEventProcessingState.FAIL);
        
        assertThat("expected fail", success, is(false));
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
                storedWorkerCodes);
        store(201, 400, StatusEventProcessingState.READY, otherEventWorkerCodes);
        
        operation.accept(sse.getId());
        
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000));
        
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
        assertThat("ids don't match", ret.getId(), is(sse.getId()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(expectedWorkerCodes));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", ret.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(1000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(expectedWorkerCodes));
    }
    
    @Test
    public void getAndSetProcessingSortWithCodes() throws Exception {
        // tests that earlier events with different codes will be skipped
        // and that the newest event with matching codes is returned
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
                set("bar"));
        
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(100000));
        
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
        assertThat("ids don't match", ret.getId(), is(sse.getId()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("bar")));
        
        final StoredStatusEvent got = storage.get(sse.getId()).get();
        assertThat("incorrect state", got.getState(), is(StatusEventProcessingState.PROC));
        assertThat("incorrect updater", got.getUpdater(), is(Optional.of("whee")));
        assertThat("incorrect update time", ret.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(100000))));
        assertThat("incorrect event", got.getEvent(), is(StatusEvent.getBuilder(
                sot, Instant.ofEpochMilli(11000), StatusEventType.COPY_ACCESS_GROUP)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("foo")
                .withNullableObjectID("bar")
                .withNullableVersion(7)
                .build()));
        assertThat("ids don't match", got.getId(), is(sse.getId()));
        assertThat("incorrect worker codes", sse.getWorkerCodes(), is(set("bar")));
    }
    
    @Test
    public void getAndSetProcessingNonExistantOnState() throws Exception {
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
                null);
        
        when(clock.instant()).thenReturn(Instant.now());
        
        final Optional<StoredStatusEvent> ret = storage.setAndGetProcessingState(
                StatusEventProcessingState.READY, null, StatusEventProcessingState.PROC, "whee");
        assertThat("expected absent", ret, is(Optional.absent()));
    }
    
    @Test
    public void getAndSetProcessingNonExistantOnWorkerCode() throws Exception {
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
                storage.setProcessingState(event.getId(), oldState, newState);
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
                    state, workerCodes);
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
        failStore(null, StatusEventProcessingState.UNINDX, null,
                new NullPointerException("newEvent"));
        final StatusEvent event = StatusEvent.getBuilder(
                "Ws", Instant.ofEpochMilli(10000), StatusEventType.NEW_ALL_VERSIONS).build();
        failStore(event, null, null, new NullPointerException("state"));
        failStore(event, StatusEventProcessingState.UNINDX, set("foo", null),
                new IllegalArgumentException("null or whitespace only item in workerCodes"));
        failStore(event, StatusEventProcessingState.UNINDX, set("foo", "   \t   \n  "),
                new IllegalArgumentException("null or whitespace only item in workerCodes"));
    }
    
    private void failStore(
            final StatusEvent event,
            final StatusEventProcessingState state,
            final Set<String> workerCodes,
            final Exception expected) {
        try {
            storage.store(event, state, workerCodes);
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
    public void getByStateFail() {
        try {
            storage.get(null, -1);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("state"));
        }
    }
    
    @Test
    public void setStateFail() {
        failSetState(null, StatusEventProcessingState.INDX, new NullPointerException("id"));
        failSetState(new StatusEventID("foo"), null, new NullPointerException("newState"));
    }
    
    private void failSetState(
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
    public void indexes() {
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
                        .append("key", new Document("_id", 1))
                        .append("name", "_id_")
                        .append("ns", "test_mongostorage.searchEvents")
                )));
    }
}
