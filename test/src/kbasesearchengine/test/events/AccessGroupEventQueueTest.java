package kbasesearchengine.test.events;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import kbasesearchengine.events.AccessGroupEventQueue;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.test.common.TestCommon;

public class AccessGroupEventQueueTest {

    /* this assert does not mutate the queue state */
    private void assertQueueState(
            final AccessGroupEventQueue queue,
            final Set<StoredStatusEvent> ready,
            final Set<StoredStatusEvent> processing,
            final int size) {
        assertThat("incorrect ready", queue.getReadyForProcessing(), is(ready));
        assertThat("incorrect get processing", queue.getProcessing(), is(processing));
        assertThat("incorrect size", queue.size(), is(size));
        assertThat("incorrect isEmpty", queue.isEmpty(), is(size == 0));
    }
    
    /* this assert does not mutate the queue state */
    private void assertEmpty(final AccessGroupEventQueue queue) {
        assertQueueState(queue, set(), set(), 0);
        assertMoveToReadyCorrect(queue, set());
        assertMoveToProcessingCorrect(queue, set());
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToProcessingCorrect(
            final AccessGroupEventQueue queue,
            final Set<StoredStatusEvent> moveToProcessing) {
        assertThat("incorrect move", queue.moveReadyToProcessing(), is(moveToProcessing));
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToReadyCorrect(
            final AccessGroupEventQueue queue,
            final Set<StoredStatusEvent> moveToReady) {
        assertThat("incorrect move", queue.moveToReady(), is(moveToReady));
    }
    
    private StoredStatusEvent createEvent(
            final String id,
            final Instant time,
            final StatusEventType type,
            final StatusEventProcessingState state,
            final String objectID) {
        return StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "storagecode", time, type)
                .withNullableObjectID(objectID)
                .build(),
                new StatusEventID(id), state).build();
    }
    
    private StoredStatusEvent unproc(
            final String id,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(id, time, type, StatusEventProcessingState.UNPROC, objectID);
    }
    
    private StoredStatusEvent ready(
            final String id,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(id, time, type, StatusEventProcessingState.READY, objectID);
    }
    
    private StoredStatusEvent proc(
            final String id,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(id, time, type, StatusEventProcessingState.PROC, objectID);
    }
    
    private StoredStatusEvent unprocVer(
            final String id,
            final Instant time,
            final String objectID) {
        return createEvent(id, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.UNPROC, objectID);
    }
    
    private StoredStatusEvent readyVer(
            final String id,
            final Instant time,
            final String objectID) {
        return createEvent(id, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.READY, objectID);
    }
    
    private StoredStatusEvent procVer(
            final String id,
            final Instant time,
            final String objectID) {
        return createEvent(id, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.PROC, objectID);
    }
    
    private StoredStatusEvent loadUnproc(
            final AccessGroupEventQueue queue,
            final String id,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        final StoredStatusEvent e = unproc(id, time, objectID, type);
        queue.load(e);
        return e;
    }
    
    private StoredStatusEvent loadUnprocVer(
            final AccessGroupEventQueue queue,
            final String id,
            final Instant time,
            final String objectID) {
        final StoredStatusEvent e = unprocVer(id, time, objectID);
        queue.load(e);
        return e;
    }
    
    @Test
    public void createEmpty() {
        assertEmpty(new AccessGroupEventQueue());
        assertEmpty(new AccessGroupEventQueue(Collections.emptyList()));
    }
    
    @Test
    public void multipleEventTypes() throws Exception {
        /* tests a queue loaded with a bunch of different events blocking each other. This tests:
         * * blocking a version level event with an object level event
         * * blocking both object level and version level events with an access group level event
         * * blocking an access level event with version level and object level events with an
         *   earlier timestamp
         *   
         * These are all the combinations of blocking that are possible other than same level
         * blocks. Object /version level blocks are tested in ObjectEventQueueTest.
         */
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        
        final StoredStatusEvent e1 = loadUnproc(
                q, "1", Instant.ofEpochMilli(100000), null, StatusEventType.COPY_ACCESS_GROUP);

        final StoredStatusEvent e2 = loadUnprocVer(q, "2", Instant.ofEpochMilli(50000), "24");
        final StoredStatusEvent e3 = loadUnproc(q, "3", Instant.ofEpochMilli(60000), "24",
                StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e4 = loadUnprocVer(q, "4", Instant.ofEpochMilli(70000), "24");
        final StoredStatusEvent e5 = loadUnprocVer(q, "5", Instant.ofEpochMilli(80000), "24");
        final StoredStatusEvent e6 = loadUnproc(q, "6", Instant.ofEpochMilli(110000), "24",
                StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e7 = loadUnprocVer(q, "7", Instant.ofEpochMilli(200000), "24");
        
        final StoredStatusEvent e8 = loadUnprocVer(q, "8", Instant.ofEpochMilli(50000), "25");
        final StoredStatusEvent e9 = loadUnprocVer(q, "9", Instant.ofEpochMilli(60000), "25");
        final StoredStatusEvent e10 = loadUnprocVer(q, "10", Instant.ofEpochMilli(105000), "25");
        final StoredStatusEvent e11 = loadUnprocVer(q, "11", Instant.ofEpochMilli(120000), "25");
        final StoredStatusEvent e12 = loadUnprocVer(q, "12", Instant.ofEpochMilli(170000), "25");
        final StoredStatusEvent e13 = loadUnprocVer(q, "13", Instant.ofEpochMilli(200000), "25");
        
        final StoredStatusEvent e20 = loadUnproc(
                q, "20", Instant.ofEpochMilli(150000), null, StatusEventType.PUBLISH_ACCESS_GROUP);
        
        assertQueueState(q, set(), set(), 14);
        assertMoveToReadyCorrect(q, set(e2, e8, e9));
        assertQueueState(q, set(e2, e8, e9), set(), 14);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e2, e8, e9));
        assertQueueState(q, set(), set(e2, e8, e9), 14);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e8);
        assertQueueState(q, set(), set(e2, e9), 13);
        
        q.setProcessingComplete(e2);
        assertQueueState(q, set(e3), set(e9), 12);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e3));
        assertQueueState(q, set(), set(e3, e9), 12);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e9);
        assertQueueState(q, set(), set(e3), 11);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e3);
        assertQueueState(q, set(e4, e5), set(), 10);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e4, e5));
        assertQueueState(q, set(), set(e4, e5), 10);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e4);
        assertQueueState(q, set(), set(e5), 9);
        q.setProcessingComplete(e5);
        assertQueueState(q, set(e1), set(), 8);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e1));
        assertQueueState(q, set(), set(e1), 8);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e6, e10, e11), set(), 7);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e6, e10, e11));
        assertQueueState(q, set(), set(e6, e10, e11), 7);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e10);
        assertQueueState(q, set(), set(e6, e11), 6);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());

        q.setProcessingComplete(e11);
        assertQueueState(q, set(), set(e6), 5);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e6);
        assertQueueState(q, set(e20), set(), 4);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e20));
        assertQueueState(q, set(), set(e20), 4);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e20);
        assertQueueState(q, set(e7, e12, e13), set(), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e7, e12, e13));
        assertQueueState(q, set(), set(e7, e12, e13), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e7);
        assertQueueState(q, set(), set(e12, e13), 2);
        q.setProcessingComplete(e12);
        assertQueueState(q, set(), set(e13), 1);
        q.setProcessingComplete(e13);
        
        assertEmpty(q);
    }
    
    @Test
    public void blockAccessGroupEvent() {
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        
        final StoredStatusEvent e3 = loadUnproc(
                q, "3", Instant.ofEpochMilli(200000), null, StatusEventType.PUBLISH_ACCESS_GROUP);
        final StoredStatusEvent e1 = loadUnproc(
                q, "1", Instant.ofEpochMilli(100000), null, StatusEventType.PUBLISH_ACCESS_GROUP);
        final StoredStatusEvent e2 = loadUnproc(
                q, "2", Instant.ofEpochMilli(150000), null, StatusEventType.PUBLISH_ACCESS_GROUP);
        
        assertQueueState(q, set(), set(), 3);
        assertMoveToReadyCorrect(q, set(e1));
        assertQueueState(q, set(e1), set(), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e1));
        assertQueueState(q, set(), set(e1), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e2), set(), 2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e2));
        assertQueueState(q, set(), set(e2), 2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e2);
        assertQueueState(q, set(e3), set(), 1);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e3));
        assertQueueState(q, set(), set(e3), 1);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e3);
        assertEmpty(q);
    }
    
    @Test
    public void blockQueueWithAccessGroupTypes() {
        blockQueueWithAccessGroupType(StatusEventType.COPY_ACCESS_GROUP);
        blockQueueWithAccessGroupType(StatusEventType.DELETE_ACCESS_GROUP);
        blockQueueWithAccessGroupType(StatusEventType.PUBLISH_ACCESS_GROUP);
        blockQueueWithAccessGroupType(StatusEventType.UNPUBLISH_ACCESS_GROUP);
    }

    private void blockQueueWithAccessGroupType(final StatusEventType type) {
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        
        final StoredStatusEvent e1 = loadUnproc(
                q, "3", Instant.ofEpochMilli(10000), null, type);
        final StoredStatusEvent e2 = loadUnproc(q, "4", Instant.ofEpochMilli(20000), "1",
                StatusEventType.PUBLISH_ALL_VERSIONS);
        
        assertMoveToReadyCorrect(q, set(e1));
        assertQueueState(q, set(e1), set(), 2);
        assertMoveToProcessingCorrect(q, set(e1));
        assertQueueState(q, set(), set(e1), 2);
        
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e2), set(), 1);
    }
    
    @Test
    public void setProcessedWithMutatedEvent() {
        // in practice we expect the events passed into setProcessed() to have mutated slightly
        // from the original load()ed event, so check that works.
        // the status event itself and the id should not mutate, but other fields are fair game.
        
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        q.load(sse);
        q.moveToReady();
        q.moveReadyToProcessing();
        assertQueueState(q, set(), set(sse), 1);
        
        final StoredStatusEvent hideousmutant = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.INDX)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "whee")
                .build();
        
        q.setProcessingComplete(hideousmutant);
        assertEmpty(q);
    }
    
    @Test
    public void immutableGetReady() {
        // test the 3 getReady paths
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("bar")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        final StoredStatusEvent sse3 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC).build();
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(sse));
        assertGetReadyReturnIsImmutable(sse2, q);
        
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue(Arrays.asList(sse2));
        assertGetReadyReturnIsImmutable(sse, q2);
        
        final AccessGroupEventQueue q3 = new AccessGroupEventQueue(Arrays.asList(sse3));
        assertGetReadyReturnIsImmutable(sse, q3);
    }

    private void assertGetReadyReturnIsImmutable(
            final StoredStatusEvent sse,
            final AccessGroupEventQueue q) {
        try {
            q.getReadyForProcessing().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            //test passed
        }
    }
    
    @Test
    public void immutableGetProcessing() {
        // test the 3 getProcessing paths
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC).build();
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("bar")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC).build();
        final StoredStatusEvent sse3 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(sse));
        assertGetProcessingReturnIsImmutable(sse2, q);
        
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue(Arrays.asList(sse2));
        assertGetProcessingReturnIsImmutable(sse, q2);
        
        final AccessGroupEventQueue q3 = new AccessGroupEventQueue(Arrays.asList(sse3));
        assertGetProcessingReturnIsImmutable(sse, q3);
    }

    private void assertGetProcessingReturnIsImmutable(
            final StoredStatusEvent sse,
            final AccessGroupEventQueue q) {
        try {
            q.getProcessing().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            //test passed
        }
    }
    
    @Test
    public void immutableMoveReady() {
        // test both moveReady paths
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        q.load(sse);
        assertMoveReadyReturnIsImmutable(sse2, q);
        
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue();
        q2.load(sse);
        q2.moveToReady();
        assertMoveReadyReturnIsImmutable(sse2, q2);
    }

    private void assertMoveReadyReturnIsImmutable(
            final StoredStatusEvent sse,
            final AccessGroupEventQueue q) {
        try {
            q.moveToReady().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void immutableMoveProcessing() {
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(sse));
        assertMoveProcessingReturnIsImmutable(sse2, q);
        
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue(Arrays.asList(sse2));
        assertMoveProcessingReturnIsImmutable(sse, q2);
    }

    private void assertMoveProcessingReturnIsImmutable(
            final StoredStatusEvent sse,
            final AccessGroupEventQueue q) {
        try {
            q.moveReadyToProcessing().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void loadFail() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        final StatusEventID id = new StatusEventID("some id");
        
        //null
        failLoad(null, new NullPointerException("event"));
        
        //bad state
        for (final StatusEventProcessingState state: Arrays.asList(
                StatusEventProcessingState.FAIL, StatusEventProcessingState.INDX,
                StatusEventProcessingState.UNINDX, StatusEventProcessingState.READY,
                StatusEventProcessingState.PROC)) {
            failLoad(StoredStatusEvent.getBuilder(se, id, state).build(),
                    new IllegalArgumentException("Illegal state for loading event: " + state));
        }
    }
    
    private void failLoad(
            final StoredStatusEvent event,
            final Exception expected) {
        try {
            new AccessGroupEventQueue().load(event);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void setProcessingCompleteFail() {
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        
        //nulls
        failSetProcessingComplete(q, null, new NullPointerException("event"));
        
        //empty queue
        failSetProcessingComplete(q, sse, new NoSuchEventException(sse));
        
        // with group level event in processed state with different event id
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue(Arrays.asList(
                StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC).build()));
        failSetProcessingComplete(q2, sse, new NoSuchEventException(sse));
        
        // with object level event with different object id
        final AccessGroupEventQueue q3 = new AccessGroupEventQueue(Arrays.asList(
                StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                        .withNullableObjectID("id2")
                        .build(),
                        new StatusEventID("foo"), StatusEventProcessingState.PROC).build()));
        failSetProcessingComplete(q3, sse, new NoSuchEventException(sse));
        
        
        // with version level event with different event id
        final AccessGroupEventQueue q4= new AccessGroupEventQueue(Arrays.asList(
                StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                        .withNullableObjectID("id")
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC).build()));
        failSetProcessingComplete(q4, sse, new NoSuchEventException(sse));
    }
    
    private void failSetProcessingComplete(
            final AccessGroupEventQueue queue,
            final StoredStatusEvent sse,
            final Exception expected) {
        try {
            queue.setProcessingComplete(sse);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    @Test
    public void constructWithAccessGroupReady() {
        constructWithAccessGroupReady(StatusEventType.COPY_ACCESS_GROUP);
        constructWithAccessGroupReady(StatusEventType.DELETE_ACCESS_GROUP);
        constructWithAccessGroupReady(StatusEventType.PUBLISH_ACCESS_GROUP);
        constructWithAccessGroupReady(StatusEventType.UNPUBLISH_ACCESS_GROUP);
    }

    private void constructWithAccessGroupReady(final StatusEventType type) {
        final StoredStatusEvent e1 = ready(
                "1", Instant.ofEpochMilli(20000), null, type);
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(e1));
        
        assertQueueState(q, set(e1), set(), 1);
        
        // add an object level event to make sure it's blocked
        final StoredStatusEvent e2 = unproc("2", Instant.ofEpochMilli(10000), "foo",
                StatusEventType.DELETE_ALL_VERSIONS);
        q.load(e2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e1));
        assertQueueState(q, set(), set(e1), 2);
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e2), set(), 1);
    }
    
    @Test
    public void constructWithAccessGroupProc() {
        constructWithAccessGroupProc(StatusEventType.COPY_ACCESS_GROUP);
        constructWithAccessGroupProc(StatusEventType.DELETE_ACCESS_GROUP);
        constructWithAccessGroupProc(StatusEventType.PUBLISH_ACCESS_GROUP);
        constructWithAccessGroupProc(StatusEventType.UNPUBLISH_ACCESS_GROUP);
    }

    private void constructWithAccessGroupProc(final StatusEventType type) {
        final StoredStatusEvent e1 = proc(
                "1", Instant.ofEpochMilli(20000), null, type);
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(e1));
        
        assertQueueState(q, set(), set(e1), 1);
        
        // add an object level event to make sure it's blocked
        final StoredStatusEvent e2 = unproc("2", Instant.ofEpochMilli(10000), "foo",
                StatusEventType.DELETE_ALL_VERSIONS);
        q.load(e2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        assertQueueState(q, set(), set(e1), 2);
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e2), set(), 1);
    }
    
    @Test
    public void constructWithMultipleObjectAndVersionProcAndReady() {
        final StoredStatusEvent e1 = ready("1", Instant.ofEpochMilli(20000), "foo1",
                StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e2 = ready("2", Instant.ofEpochMilli(20000), "foo2",
                StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e3 = proc("3", Instant.ofEpochMilli(20000), "foo3",
                StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e4 = readyVer("4", Instant.ofEpochMilli(20000), "foo4");
        final StoredStatusEvent e5 = procVer("5", Instant.ofEpochMilli(20000), "foo4");
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(
                Arrays.asList(e1, e2, e3, e4, e5));
        
        assertQueueState(q, set(e1, e2, e4), set(e3, e5), 5);
        
        // add access group, object and version events to make sure they're blocked
        final StoredStatusEvent e6 = unproc(
                "6", Instant.ofEpochMilli(150000), null, StatusEventType.COPY_ACCESS_GROUP);
        final StoredStatusEvent e7 = unproc(
                "7", Instant.ofEpochMilli(10000), "foo4", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e8 = unprocVer("8", Instant.ofEpochMilli(10000), "foo1");
        q.load(e6);
        q.load(e7);
        q.load(e8);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e1, e2, e4));
        assertQueueState(q, set(), set(e1, e2, e3, e4, e5), 8);
        
        q.setProcessingComplete(e1);
        assertQueueState(q, set(e8), set(e2, e3, e4, e5), 7);
        q.setProcessingComplete(e4);
        assertQueueState(q, set(e8), set(e2, e3, e5), 6);
        q.setProcessingComplete(e5);
        assertQueueState(q, set(e8, e7), set(e2, e3), 5);
        
        assertMoveToProcessingCorrect(q, set(e8, e7));
        q.setProcessingComplete(e7);
        q.setProcessingComplete(e8);
        q.setProcessingComplete(e2);
        assertQueueState(q, set(), set(e3), 2);
        
        q.setProcessingComplete(e3);
        assertQueueState(q, set(e6), set(), 1);
    }
    
    @Test
    public void constructWithVersionProcOrReadyOnly() {
        // tests adding more than one version of the same type to an object queue,
        // but not adding the other type to ensure that null pointers don't occur for the missing
        // type
        final StoredStatusEvent e1 = readyVer("1", Instant.ofEpochMilli(20000), "foo1");
        final StoredStatusEvent e2 = readyVer("2", Instant.ofEpochMilli(20000), "foo1");
        final StoredStatusEvent e3 = procVer("3", Instant.ofEpochMilli(20000), "foo2");
        final StoredStatusEvent e4 = procVer("4", Instant.ofEpochMilli(20000), "foo2");
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue(Arrays.asList(e1, e2, e3, e4));
        
        assertQueueState(q, set(e1, e2), set(e3, e4), 4);
    }
    
    @Test
    public void constructFailNulls() {
        final StoredStatusEvent e1 = readyVer("6", Instant.ofEpochMilli(20000), "foo1");
        failConstruct(new NullPointerException("initialLoad"), (List<StoredStatusEvent>) null);
        failConstruct(new NullPointerException("initialLoad has null entries"),
                Arrays.asList(e1, null));
    }
    
    @Test
    public void constructFailIllegalEventLevelCombinations() {
        // test combinations of illegal event types.
        final StoredStatusEvent agready = ready(
                "1", Instant.ofEpochMilli(20000), null, StatusEventType.COPY_ACCESS_GROUP);
        final StoredStatusEvent agproc = proc(
                "2", Instant.ofEpochMilli(20000), null, StatusEventType.COPY_ACCESS_GROUP);
        final StoredStatusEvent objready1 = ready(
                "3", Instant.ofEpochMilli(10000), "foo1", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent objproc1 = ready(
                "4", Instant.ofEpochMilli(10000), "foo1", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent verready1 = readyVer("5", Instant.ofEpochMilli(20000), "foo1");
        final StoredStatusEvent verproc1 = readyVer("6", Instant.ofEpochMilli(20000), "foo1");
        
        final IllegalArgumentException expAccess = new IllegalArgumentException(
                "More than one access level event is not allowed");
        failConstruct(expAccess, agready, agproc);
        failConstruct(expAccess, agproc, agready);
        failConstruct(expAccess, agready, agready);
        failConstruct(expAccess, agproc, agproc);
        
        final IllegalArgumentException expAccessPlus = new IllegalArgumentException(
                "If an access group level event is in the ready or processing state, no " +
                "other events may be submitted");
        failConstruct(expAccessPlus, agready, objready1);
        failConstruct(expAccessPlus, agready, objproc1);
        failConstruct(expAccessPlus, agready, verready1);
        failConstruct(expAccessPlus, agready, verproc1);
        failConstruct(expAccessPlus, agproc, objready1);
        failConstruct(expAccessPlus, agproc, objproc1);
        failConstruct(expAccessPlus, agproc, verready1);
        failConstruct(expAccessPlus, agproc, verproc1);
        
        final IllegalArgumentException expObjVer = new IllegalArgumentException(
                "Cannot submit both object and version level events for object ID foo1");
        failConstruct(expObjVer, objready1, verready1);
        failConstruct(expObjVer, objready1, verproc1);
        failConstruct(expObjVer, objproc1, verready1);
        failConstruct(expObjVer, objproc1, verproc1);
        
        final IllegalArgumentException expObj2 = new IllegalArgumentException(
                "Already contains an event for object ID foo1");
        failConstruct(expObj2, objready1, objproc1);
    }
    
    @Test
    public void constructFailIllegalState() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION).build();
        final StatusEventID id = new StatusEventID("some id");
        
        for (final StatusEventProcessingState state: Arrays.asList(
                StatusEventProcessingState.FAIL, StatusEventProcessingState.INDX,
                StatusEventProcessingState.UNINDX, StatusEventProcessingState.UNPROC)) {
            failConstruct(new IllegalArgumentException("Illegal initial event state: " + state),
                    StoredStatusEvent.getBuilder(se, id, state).build());
        }
    }
    
    private void failConstruct(final Exception expected, final List<StoredStatusEvent> events) {
        try {
            new AccessGroupEventQueue(events);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failConstruct(final Exception expected, final StoredStatusEvent... events) {
        try {
            new AccessGroupEventQueue(Arrays.asList(events));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
