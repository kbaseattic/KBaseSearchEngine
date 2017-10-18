package kbasesearchengine.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.StatusEvent.Builder;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;

public class StatusEventTest {
    
    @Test
    public void minimalBuildStorageCode() {
        final StatusEvent se = StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.NEW_ALL_VERSIONS)
                .build();
        
        assertThat("incorrect access group id", se.getAccessGroupId(), is(Optional.absent()));
        assertThat("incorrect access group object id", se.getAccessGroupObjectId(),
                is(Optional.absent()));
        assertThat("incorrect event type", se.getEventType(),
                is(StatusEventType.NEW_ALL_VERSIONS));
        assertThat("incorrect new name", se.getNewName(), is(Optional.absent()));
        assertThat("incorrect processing state", se.getProcessingState(),
                is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect storage code", se.getStorageCode(), is("WS"));
        assertThat("incorrect storage object type", se.getStorageObjectType(),
                is(Optional.absent()));
        assertThat("incorrect timestamp", se.getTimestamp(), is(Instant.ofEpochMilli(20000)));
        assertThat("incorrect version", se.getVersion(), is(Optional.absent()));
        assertThat("incorrect is public", se.isPublic(), is(Optional.absent()));
        //TODO CODE this is clearly an invalid GUID. Need to do something else here.
        assertThat("incorrect guid", se.toGUID(),
                is((new GUID("WS", null, null, null, null, null))));
        assertThat("incorrect toString", se.toString(),
                is("StatusEvent [time=1970-01-01T00:00:20Z, eventType=NEW_ALL_VERSIONS, " +
                        "storageCode=WS, storageObjectType=Optional.absent(), " +
                        "accessGroupID=Optional.absent(), objectID=Optional.absent(), " +
                        "version=Optional.absent(), isPublic=Optional.absent(), " +
                        "newName=Optional.absent()]"));
    }
    
    @Test
    public void maximalBuildStorageType() {
        final StatusEvent se = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("RK", "foo", 3),
                Instant.ofEpochMilli(30000), StatusEventType.RENAME_ALL_VERSIONS)
                .withProcessingState(StatusEventProcessingState.FAIL)
                .withNullableAccessGroupID(6)
                .withNullableisPublic(true)
                .withNullableNewName("nn")
                .withNullableObjectID("2")
                .withNullableVersion(8)
                .build();
        
        assertThat("incorrect access group id", se.getAccessGroupId(), is(Optional.of(6)));
        assertThat("incorrect access group object id", se.getAccessGroupObjectId(),
                is(Optional.of("2")));
        assertThat("incorrect event type", se.getEventType(),
                is(StatusEventType.RENAME_ALL_VERSIONS));
        assertThat("incorrect new name", se.getNewName(), is(Optional.of("nn")));
        assertThat("incorrect processing state", se.getProcessingState(),
                is(StatusEventProcessingState.FAIL));
        assertThat("incorrect storage code", se.getStorageCode(), is("RK"));
        assertThat("incorrect storage object type", se.getStorageObjectType(),
                is(Optional.of(StorageObjectType.fromNullableVersion("RK", "foo", 3))));
        assertThat("incorrect timestamp", se.getTimestamp(), is(Instant.ofEpochMilli(30000)));
        assertThat("incorrect version", se.getVersion(), is(Optional.of(8)));
        assertThat("incorrect is public", se.isPublic(), is(Optional.of(true)));
        assertThat("incorrect guid", se.toGUID(),
                is((new GUID("RK", 6, "2", 8, null, null))));
        assertThat("incorrect toString", se.toString(),
                is("StatusEvent [time=1970-01-01T00:00:30Z, eventType=RENAME_ALL_VERSIONS, " +
                        "storageCode=RK, storageObjectType=Optional.of(StorageObjectType " +
                        "[storageCode=RK, type=foo, version=Optional.of(3)]), " +
                        "accessGroupID=Optional.of(6), objectID=Optional.of(2), " +
                        "version=Optional.of(8), isPublic=Optional.of(true), " +
                        "newName=Optional.of(nn)]"));
    }
    
    @Test
    public void nullableBuild() {
        final StatusEvent se = StatusEvent.getBuilder(
                "PP", Instant.ofEpochMilli(40000), StatusEventType.NEW_VERSION)
                .withNullableAccessGroupID(null)
                .withNullableisPublic(null)
                .withNullableNewName(null)
                .withNullableObjectID(null)
                .withNullableVersion(null)
                .build();
        
        assertThat("incorrect access group id", se.getAccessGroupId(), is(Optional.absent()));
        assertThat("incorrect access group object id", se.getAccessGroupObjectId(),
                is(Optional.absent()));
        assertThat("incorrect event type", se.getEventType(),
                is(StatusEventType.NEW_VERSION));
        assertThat("incorrect new name", se.getNewName(), is(Optional.absent()));
        assertThat("incorrect processing state", se.getProcessingState(),
                is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect storage code", se.getStorageCode(), is("PP"));
        assertThat("incorrect storage object type", se.getStorageObjectType(),
                is(Optional.absent()));
        assertThat("incorrect timestamp", se.getTimestamp(), is(Instant.ofEpochMilli(40000)));
        assertThat("incorrect version", se.getVersion(), is(Optional.absent()));
        assertThat("incorrect is public", se.isPublic(), is(Optional.absent()));
        assertThat("incorrect guid", se.toGUID(),
                is((new GUID("PP", null, null, null, null, null))));
        assertThat("incorrect toString", se.toString(),
                is("StatusEvent [time=1970-01-01T00:00:40Z, eventType=NEW_VERSION, " +
                        "storageCode=PP, storageObjectType=Optional.absent(), " +
                        "accessGroupID=Optional.absent(), objectID=Optional.absent(), " +
                        "version=Optional.absent(), isPublic=Optional.absent(), " +
                        "newName=Optional.absent()]"));
    }
    
    @Test
    public void emptyStringBuild() {
        final StatusEvent se = StatusEvent.getBuilder(
                "PP", Instant.ofEpochMilli(40000), StatusEventType.NEW_VERSION)
                .withNullableNewName("   \t ")
                .withNullableObjectID("   \n ")
                .build();
        
        assertThat("incorrect access group id", se.getAccessGroupId(), is(Optional.absent()));
        assertThat("incorrect access group object id", se.getAccessGroupObjectId(),
                is(Optional.absent()));
        assertThat("incorrect event type", se.getEventType(),
                is(StatusEventType.NEW_VERSION));
        assertThat("incorrect new name", se.getNewName(), is(Optional.absent()));
        assertThat("incorrect processing state", se.getProcessingState(),
                is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect storage code", se.getStorageCode(), is("PP"));
        assertThat("incorrect storage object type", se.getStorageObjectType(),
                is(Optional.absent()));
        assertThat("incorrect timestamp", se.getTimestamp(), is(Instant.ofEpochMilli(40000)));
        assertThat("incorrect version", se.getVersion(), is(Optional.absent()));
        assertThat("incorrect is public", se.isPublic(), is(Optional.absent()));
        assertThat("incorrect guid", se.toGUID(),
                is((new GUID("PP", null, null, null, null, null))));
        assertThat("incorrect toString", se.toString(),
                is("StatusEvent [time=1970-01-01T00:00:40Z, eventType=NEW_VERSION, " +
                        "storageCode=PP, storageObjectType=Optional.absent(), " +
                        "accessGroupID=Optional.absent(), objectID=Optional.absent(), " +
                        "version=Optional.absent(), isPublic=Optional.absent(), " +
                        "newName=Optional.absent()]"));
    }
    
    @Test
    public void buildFail() {
        // only one way the build can fail other than getBuilder method
        final Builder se = StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.NEW_ALL_VERSIONS);
        try {
            se.withProcessingState(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("state"));
        }
    }
    
    @Test
    public void getBuilderFail() {
        failGetBuilder((String) null, Instant.ofEpochMilli(20000),
                StatusEventType.NEW_ALL_VERSIONS,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failGetBuilder("   \t ", Instant.ofEpochMilli(20000), StatusEventType.NEW_ALL_VERSIONS,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failGetBuilder("WS", null, StatusEventType.NEW_ALL_VERSIONS,
                new NullPointerException("time"));
        failGetBuilder("WS", Instant.ofEpochMilli(20000), null,
                new NullPointerException("eventType"));
        
        final StorageObjectType sot = StorageObjectType.fromNullableVersion("RK", "foo", 3);
        
        failGetBuilder((StorageObjectType) null, Instant.ofEpochMilli(20000),
                StatusEventType.NEW_ALL_VERSIONS,
                new NullPointerException("storageType"));
        failGetBuilder(sot, null, StatusEventType.NEW_ALL_VERSIONS,
                new NullPointerException("time"));
        failGetBuilder(sot, Instant.ofEpochMilli(20000), null,
                new NullPointerException("eventType"));
        
    }

    private void failGetBuilder(
            final StorageObjectType sot,
            final Instant time,
            final StatusEventType type,
            final Exception expected) {
        try {
            StatusEvent.getBuilder(sot, time, type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    private void failGetBuilder(
            final String storageCode,
            final Instant time,
            final StatusEventType type,
            final Exception expected) {
        try {
            StatusEvent.getBuilder(storageCode, time, type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

}
