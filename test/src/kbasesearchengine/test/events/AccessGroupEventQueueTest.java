package kbasesearchengine.test.events;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
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
        return new StoredStatusEvent(StatusEvent.getBuilder(
                "storagecode", time, type)
                .withNullableObjectID(objectID)
                .build(),
                new StatusEventID(id), state, null, null);
    }
    
    private StoredStatusEvent unproc(
            final String id,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(id, time, type, StatusEventProcessingState.UNPROC, objectID);
    }
    
    private StoredStatusEvent unprocVer(
            final String id,
            final Instant time,
            final String objectID) {
        return createEvent(id, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.UNPROC, objectID);
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
    public void setProcessedWithMutatedEvent() {
        // in practice we expect the events passed into setProcessed() to have mutated slightly
        // from the original load()ed event, so check that works.
        // the status event itself and the id should not mutate, but other fields are fair game.
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
        final AccessGroupEventQueue q = new AccessGroupEventQueue();
        q.load(sse);
        q.moveToReady();
        q.moveReadyToProcessing();
        assertQueueState(q, set(), set(sse), 1);
        
        final StoredStatusEvent hideousmutant = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.INDX,
                Instant.ofEpochMilli(10000), "whee");
        
        q.setProcessingComplete(hideousmutant);
        assertEmpty(q);
    }
    
    @Test
    public void immutableGetReady() {
        // test the 3 getReady paths
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("bar")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse3 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        
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
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("bar")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        final StoredStatusEvent sse3 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
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
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
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
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
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
            failLoad(new StoredStatusEvent(se, id, state, null, null),
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
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
        //nulls
        failSetProcessingComplete(q, null, new NullPointerException("event"));
        
        //empty queue
        failSetProcessingComplete(q, sse, new NoSuchEventException(sse));
        
        // with group level event in processed state with different event id
        final AccessGroupEventQueue q2 = new AccessGroupEventQueue(Arrays.asList(
                new StoredStatusEvent(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC, null, null)));
        failSetProcessingComplete(q2, sse, new NoSuchEventException(sse));
        
        // with object level event with different object id
        final AccessGroupEventQueue q3 = new AccessGroupEventQueue(Arrays.asList(
                new StoredStatusEvent(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                        .withNullableObjectID("id2")
                        .build(),
                        new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null)));
        failSetProcessingComplete(q3, sse, new NoSuchEventException(sse));
        
        
        // with version level event with different event id
        final AccessGroupEventQueue q4= new AccessGroupEventQueue(Arrays.asList(
                new StoredStatusEvent(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                        .withNullableObjectID("id")
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC, null, null)));
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

    //TODO QUEUE NOW constructor tests
    //TODO QUEUE NOW constructor unhappy tests
    
}
