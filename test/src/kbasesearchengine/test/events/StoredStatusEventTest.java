package kbasesearchengine.test.events;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Collections;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class StoredStatusEventTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(StoredStatusEvent.class).usingGetClass().verify();
    }
    
    @Test 
    public void buildMinimal() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("bar"),
                StatusEventProcessingState.UNPROC)
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("bar")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sei.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect storeTime", sei.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=bar], state=UNPROC, updateTime=Optional.absent(), " +
                "updater=Optional.absent(), workerCodes=[], storedBy=Optional.absent(), " +
                "storeTime=Optional.absent()]"));
        
    }

    
    @Test 
    public void buildMinimalNulls() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("bar"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(null, null)
                .withNullableStoredBy(null)
                .withNullableStoredBy("   \t  \n ")
                .withNullableStoreTime(null)
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("bar")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sei.getUpdateTime(), is(Optional.absent()));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect storeTime", sei.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=bar], state=UNPROC, updateTime=Optional.absent(), " +
                "updater=Optional.absent(), workerCodes=[], storedBy=Optional.absent(), " +
                "storeTime=Optional.absent()]"));
    }
    
    @Test 
    public void buildMinimalNoUpdater() {
        buildMinimalNoUpdater(null);
        buildMinimalNoUpdater("   \t  \n  ");
    }

    private void buildMinimalNoUpdater(final String updater) {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("bar"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(Instant.ofEpochMilli(20000), updater)
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("bar")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sei.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(20000))));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect storeTime", sei.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=bar], state=UNPROC, " +
                "updateTime=Optional.of(1970-01-01T00:00:20Z), " +
                "updater=Optional.absent(), workerCodes=[], storedBy=Optional.absent(), " +
                "storeTime=Optional.absent()]"));
    }
    
    @Test
    public void buildMinimalReplaceUpdater() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("baz"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "foo")
                .withNullableUpdate(Instant.ofEpochMilli(20000), "bar")
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("baz")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.of("bar")));
        assertThat("incorrect update time", sei.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(20000))));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect storeTime", sei.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=baz], state=UNPROC, " +
                "updateTime=Optional.of(1970-01-01T00:00:20Z), " +
                "updater=Optional.of(bar), workerCodes=[], storedBy=Optional.absent(), " +
                "storeTime=Optional.absent()]"));
    }
    
    @Test
    public void buildMinimalReplaceUpdaterWithNull() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("baz"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "foo")
                .withNullableUpdate(Instant.ofEpochMilli(20000), null)
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("baz")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sei.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(20000))));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.absent()));
        assertThat("incorrect storeTime", sei.getStoreTime(), is(Optional.absent()));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=baz], state=UNPROC, " +
                "updateTime=Optional.of(1970-01-01T00:00:20Z), " +
                "updater=Optional.absent(), workerCodes=[], storedBy=Optional.absent(), " +
                "storeTime=Optional.absent()]"));
    }
    
    @Test
    public void buildMaximal() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("foo"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(Instant.ofEpochMilli(20000), "foo")
                .withWorkerCode("foo")
                .withWorkerCode("bar")
                .withNullableStoredBy("my man")
                .withNullableStoreTime(Instant.ofEpochMilli(30000))
                .build();
        assertThat("incorrect id", sei.getID(), is(new StatusEventID("foo")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.of("foo")));
        assertThat("incorrect update time", sei.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(20000))));
        assertThat("incorrect is parent", sei.isParentId(), is(false));
        assertThat("incorrect tags", sei.getWorkerCodes(), is(set("foo", "bar")));
        assertThat("incorrect storedby", sei.getStoredBy(), is(Optional.of("my man")));
        assertThat("incorrect storeTime", sei.getStoreTime(),
                is(Optional.of(Instant.ofEpochMilli(30000))));
        assertThat("incorrect toString", sei.toString(), is(
                "StoredStatusEvent [event=StatusEvent [time=1970-01-01T00:00:10Z, " +
                "eventType=DELETE_ALL_VERSIONS, storageCode=foo, " +
                "storageObjectType=Optional.absent(), accessGroupID=Optional.absent(), " +
                "objectID=Optional.absent(), version=Optional.absent(), " +
                "isPublic=Optional.absent(), newName=Optional.absent()], " +
                "id=StatusEventID [id=foo], state=UNPROC, " +
                "updateTime=Optional.of(1970-01-01T00:00:20Z), updater=Optional.of(foo), " +
                "workerCodes=[bar, foo], storedBy=Optional.of(my man), " +
                "storeTime=Optional.of(1970-01-01T00:00:30Z)]"));
    }
    
    @Test
    public void immutable() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = StoredStatusEvent.getBuilder(se, new StatusEventID("foo"),
                StatusEventProcessingState.UNPROC)
                .withNullableUpdate(Instant.ofEpochMilli(20000), "foo")
                .withWorkerCode("foo")
                .withWorkerCode("bar")
                .build();
        
        try {
            sei.getWorkerCodes().add("whee");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            //pass
        }
    }
    
    @Test
    public void buildFail() {
        final StatusEvent event = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        final StatusEventID id = new StatusEventID("foo");
        final StatusEventProcessingState state = StatusEventProcessingState.UNINDX;
        
        failBuild(null, id, state, new NullPointerException("event"));
        failBuild(event, null, state, new NullPointerException("id"));
        failBuild(event, id, null, new NullPointerException("state"));
    }
    
    @Test
    public void buildFailTag() {
        failBuildTag(null,
                new IllegalArgumentException("workerCode cannot be null or whitespace"));
        failBuildTag("   \t   \n ",
                new IllegalArgumentException("workerCode cannot be null or whitespace"));
    }
    
    private void failBuildTag(final String tag, final Exception expected) {
        final StatusEvent event = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        try {
            StoredStatusEvent.getBuilder(event, new StatusEventID("foo"),
                    StatusEventProcessingState.UNINDX)
                    .withWorkerCode(tag);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failBuild(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state,
            final Exception expected) {
        try {
            // no exceptions can be triggered by update time or updater
            StoredStatusEvent.getBuilder(event, id, state).build();
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
