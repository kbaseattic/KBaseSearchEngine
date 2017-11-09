package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static kbasesearchengine.test.common.TestCommon.set;

import org.junit.Test;

import kbasesearchengine.events.ObjectEventQueue;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.test.common.TestCommon;

public class ObjectEventQueueTest {

    /* this assert does not mutate the queue state */
    private void assertQueueState(
            final ObjectEventQueue queue,
            final Set<StoredStatusEvent> ready,
            final Set<StoredStatusEvent> processing,
            final int size) {
        assertThat("incorrect ready", queue.getReadyForProcessing(), is(ready));
        assertThat("incorrect get processing", queue.getProcessing(), is(processing));
        assertThat("incorrect is processing", queue.isProcessing(), is(!processing.isEmpty()));
        assertThat("incorrect size", queue.size(), is(size));
        assertThat("incorrect isEmpty", queue.isEmpty(), is(size == 0));
    }
    
    /* this assert does not mutate the queue state */
    private void assertEmpty(final ObjectEventQueue queue) {
        assertQueueState(queue, set(), set(), 0);
        assertMoveToReadyCorrect(queue, set());
        assertMoveToProcessingCorrect(queue, set());
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToProcessingCorrect(
            final ObjectEventQueue queue,
            final Set<StoredStatusEvent> moveToProcessing) {
        assertThat("incorrect move", queue.moveReadyToProcessing(), is(moveToProcessing));
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToReadyCorrect(
            final ObjectEventQueue queue,
            final Set<StoredStatusEvent> moveToReady) {
        assertThat("incorrect move", queue.moveToReady(), is(moveToReady));
    }
    
    @Test
    public void constructEmpty() {
        assertEmpty(new ObjectEventQueue());
    }
    
    @Test
    public void loadOneObjectLevelEventAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
        assertEmpty(q);
        
        q.load(sse);
        assertQueueState(q, set(), set(), 1);
        assertMoveToReadyCorrect(q, set(sse)); //mutates queue
        assertQueueState(q, set(sse), set(), 1);
        assertMoveToProcessingCorrect(q, set(sse)); //mutates queue
        assertQueueState(q, set(), set(sse), 1);
        
        q.setProcessingComplete(sse);
        assertEmpty(q);
    }
    
    @Test
    public void loadMultipleObjectLevelEventsAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.RENAME_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        
        assertEmpty(q);
        
        q.load(sse2);
        q.load(sse1);
        q.load(sse);
        assertQueueState(q, set(), set(), 3);
        assertMoveToReadyCorrect(q, set(sse));
        assertQueueState(q, set(sse), set(), 3);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(sse));
        assertQueueState(q, set(), set(sse), 3);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(sse); // calls move to ready
        assertQueueState(q, set(sse1), set(), 2);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(sse1));
        assertQueueState(q, set(), set(sse1), 2);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(sse1);
        assertQueueState(q, set(sse2), set(), 1);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(sse2));
        assertQueueState(q, set(), set(sse2), 1);
        //check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(sse2);
        assertEmpty(q);
    }
    
    
    @Test
    public void loadOneVersionLevelEventAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
        assertEmpty(q);
        
        q.load(sse);
        assertQueueState(q, set(), set(), 1);
        assertMoveToReadyCorrect(q, set(sse));
        assertQueueState(q, set(sse), set(), 1);
        assertMoveToProcessingCorrect(q, set(sse));
        assertQueueState(q, set(), set(sse), 1);
        
        q.setProcessingComplete(sse);
        assertEmpty(q);
    }
    
    @Test
    public void loadMultipleVersionLevelEventsAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        
        assertEmpty(q);
        
        q.load(sse2);
        q.load(sse);
        assertQueueState(q, set(), set(), 2);
        assertMoveToReadyCorrect(q, set(sse, sse2)); //mutates queue
        assertQueueState(q, set(sse, sse2), set(), 2);
        assertMoveToProcessingCorrect(q, set(sse, sse2)); //mutates queue
        assertQueueState(q, set(), set(sse, sse2), 2);
        
        q.load(sse1);
        assertQueueState(q, set(), set(sse, sse2), 3);
        
        q.setProcessingComplete(sse2); // calls move to ready
        assertQueueState(q, set(sse1), set(sse), 2);
        assertMoveToReadyCorrect(q, set()); // does not mutate queue
        assertQueueState(q, set(sse1), set(sse), 2);
        assertMoveToProcessingCorrect(q, set(sse1)); //mutates queue
        assertQueueState(q, set(), set(sse, sse1), 2);

        q.setProcessingComplete(sse1);
        assertQueueState(q, set(), set(sse), 1);

        q.setProcessingComplete(sse);
        assertEmpty(q);
    }
    
    @Test
    public void blockVersionEventsWithObjectLevelEvent() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse3 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(40000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo4"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent blocking = new StoredStatusEvent(StatusEvent.getBuilder(
                "blocker", Instant.ofEpochMilli(25000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("blk"), StatusEventProcessingState.UNPROC, null, null);
        
        assertEmpty(q);
        
        q.load(sse3);
        q.load(sse2);
        q.load(sse1);
        q.load(sse);
        q.load(blocking);
        assertQueueState(q, set(), set(), 5);
        assertMoveToReadyCorrect(q, set(sse, sse1));
        assertQueueState(q, set(sse, sse1), set(), 5);
        // check the queue is now blocked
        assertMoveToReadyCorrect(q, set());
        assertQueueState(q, set(sse, sse1), set(), 5);
        assertMoveToProcessingCorrect(q, set(sse, sse1));
        assertQueueState(q, set(), set(sse, sse1), 5);
        // check queue is still blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(sse1);
        assertQueueState(q, set(), set(sse), 4);
        // check queue is still blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(sse); // calls move to ready
        assertQueueState(q, set(blocking), set(), 3);
        // check queue is blocked
        assertMoveToReadyCorrect(q, set());
        assertQueueState(q, set(blocking), set(), 3);
        assertMoveToProcessingCorrect(q, set(blocking));
        assertQueueState(q, set(), set(blocking), 3);
        //check queue is still blocked
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        assertQueueState(q, set(), set(blocking), 3);
        
        q.setProcessingComplete(blocking); // calls move to ready
        assertQueueState(q, set(sse2, sse3), set(), 2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(sse2, sse3));
        assertQueueState(q, set(), set(sse2, sse3), 2);
        
        q.setProcessingComplete(sse2);
        q.setProcessingComplete(sse3);
        assertEmpty(q);
    }
    
    @Test
    public void constructWithReadyObjectLevelEvent() {
        for (final StatusEventType type: Arrays.asList(StatusEventType.DELETE_ALL_VERSIONS,
                StatusEventType.NEW_ALL_VERSIONS, StatusEventType.PUBLISH_ALL_VERSIONS,
                StatusEventType.RENAME_ALL_VERSIONS, StatusEventType.UNDELETE_ALL_VERSIONS)) {
            assertSingleObjectLevelReadyEvent(type);
            
        }
    }

    private void assertSingleObjectLevelReadyEvent(final StatusEventType type) {
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), type)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertQueueState(q, set(sse), set(), 1);
    }
    
    @Test
    public void constructWithProcessingObjectLevelEvent() {
        for (final StatusEventType type: Arrays.asList(StatusEventType.DELETE_ALL_VERSIONS,
                StatusEventType.NEW_ALL_VERSIONS, StatusEventType.PUBLISH_ALL_VERSIONS,
                StatusEventType.RENAME_ALL_VERSIONS, StatusEventType.UNDELETE_ALL_VERSIONS)) {
            assertSingleObjectLevelProcessingEvent(type);
            
        }
    }
    
    private void assertSingleObjectLevelProcessingEvent(final StatusEventType type) {
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), type)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertQueueState(q, set(), set(sse), 1);
    }
    
    @Test
    public void constructWithVersionLevelEvents() {
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.PROC, null, null);
        final StoredStatusEvent sse3 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar3", Instant.ofEpochMilli(40000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo4"), StatusEventProcessingState.PROC, null, null);
        
        assertEmpty(new ObjectEventQueue(new LinkedList<>(), new LinkedList<>()));
        
        final ObjectEventQueue q1 = new ObjectEventQueue(
                Arrays.asList(sse1), new LinkedList<>());
        assertQueueState(q1, set(sse1), set(), 1);
        
        final ObjectEventQueue q2 = new ObjectEventQueue(
                Arrays.asList(sse, sse1), new LinkedList<>());
        assertQueueState(q2, set(sse, sse1), set(), 2);
        
        final ObjectEventQueue q3 = new ObjectEventQueue(
                new LinkedList<>(), Arrays.asList(sse2));
        assertQueueState(q3, set(), set(sse2), 1);
        
        final ObjectEventQueue q4 = new ObjectEventQueue(
                new LinkedList<>(), Arrays.asList(sse2, sse3));
        assertQueueState(q4, set(), set(sse2, sse3), 2);
        
        final ObjectEventQueue q5 = new ObjectEventQueue(
                Arrays.asList(sse1), Arrays.asList(sse2));
        assertQueueState(q5, set(sse1), set(sse2), 2);
        
        final ObjectEventQueue q6 = new ObjectEventQueue(
                Arrays.asList(sse1, sse), Arrays.asList(sse2, sse3));
        assertQueueState(q6, set(sse1, sse), set(sse2, sse3), 4);
    }
    
    @Test
    public void setProcessedWithMutatedEvent() {
        // in practice we expect the events passed into setProcessed() to have mutated slightly
        // from the original load()ed event, so check that works.
        // the status event itself and the id should not mutate, but other fields are fair game.
        
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
        final ObjectEventQueue q = new ObjectEventQueue();
        q.load(sse);
        q.moveToReady();
        q.moveReadyToProcessing();
        assertQueueState(q, set(), set(sse), 1);
        
        final StoredStatusEvent hideousmutant = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.INDX,
                Instant.ofEpochMilli(10000), "whee");
        
        q.setProcessingComplete(hideousmutant);
        assertEmpty(q);
    }
    
    @Test
    public void immutableGetReady() {
        // test both getReady paths
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertGetReadyReturnIsImmutable(sse2, q);
        
        final ObjectEventQueue q2 = new ObjectEventQueue(Arrays.asList(sse2), new LinkedList<>());
        assertGetReadyReturnIsImmutable(sse, q2);
    }

    private void assertGetReadyReturnIsImmutable(
            final StoredStatusEvent sse,
            final ObjectEventQueue q) {
        try {
            q.getReadyForProcessing().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            //test passed
        }
    }
    
    @Test
    public void immutableGetProcessing() {
        // test both getProcessing paths
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null);
        
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertGetProcessingReturnIsImmutable(sse2, q);
        
        final ObjectEventQueue q2 = new ObjectEventQueue(new LinkedList<>(), Arrays.asList(sse2));
        assertGetProcessingReturnIsImmutable(sse, q2);
    }

    private void assertGetProcessingReturnIsImmutable(final StoredStatusEvent sse,
            final ObjectEventQueue q) {
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
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC, null, null);
        
        final ObjectEventQueue q = new ObjectEventQueue();
        q.load(sse);
        assertMoveReadyReturnIsImmutable(sse2, q);
        
        final ObjectEventQueue q2 = new ObjectEventQueue();
        q2.load(sse2);
        assertMoveReadyReturnIsImmutable(sse, q2);
    }

    private void assertMoveReadyReturnIsImmutable(
            final StoredStatusEvent sse,
            final ObjectEventQueue q) {
        try {
            q.moveToReady().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void immutableMoveProcessing() {
        // test both moveProcessing paths
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        final StoredStatusEvent sse2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertMoveProcessingReturnIsImmutable(sse2, q);
        
        final ObjectEventQueue q2 = new ObjectEventQueue(Arrays.asList(sse2), new LinkedList<>());
        assertMoveProcessingReturnIsImmutable(sse, q2);
    }

    private void assertMoveProcessingReturnIsImmutable(
            final StoredStatusEvent sse,
            final ObjectEventQueue q) {
        try {
            q.moveReadyToProcessing().add(sse);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void constructFailWithVersionLevelEvents() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION).build();
        final StatusEventID id = new StatusEventID("some id");
        final List<StoredStatusEvent> mt = new LinkedList<>();
        
        // nulls
        failConstructWithVersionLevelEvents(null, mt, new NullPointerException("initialReady"));
        failConstructWithVersionLevelEvents(mt, null,
                new NullPointerException("initialProcessing"));
        
        // bad status
        for (final StatusEventProcessingState state: Arrays.asList(
                StatusEventProcessingState.FAIL, StatusEventProcessingState.INDX,
                StatusEventProcessingState.UNINDX, StatusEventProcessingState.UNPROC)) {
            failConstructWithVersionLevelEvents(
                    Arrays.asList(new StoredStatusEvent(se, id, state, null, null)),
                    new LinkedList<>(),
                    new IllegalArgumentException("Illegal initial event state: " + state));
            failConstructWithVersionLevelEvents(new LinkedList<>(),
                    Arrays.asList(new StoredStatusEvent(se, id, state, null, null)),
                    new IllegalArgumentException("Illegal initial event state: " + state));
        }
        
        failConstructWithVersionLevelEvents(
                Arrays.asList(new StoredStatusEvent(
                        se, id, StatusEventProcessingState.PROC, null, null)),
                new LinkedList<>(),
                new IllegalArgumentException("Illegal initial event state: PROC"));
        failConstructWithVersionLevelEvents(new LinkedList<>(),
                Arrays.asList(new StoredStatusEvent(
                        se, id, StatusEventProcessingState.READY, null, null)),
                new IllegalArgumentException("Illegal initial event state: READY"));
        
        // bad event types
        for (final StatusEventType type: Arrays.asList(
                StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
                StatusEventType.DELETE_ALL_VERSIONS, StatusEventType.NEW_ALL_VERSIONS,
                StatusEventType.PUBLISH_ACCESS_GROUP, StatusEventType.PUBLISH_ALL_VERSIONS,
                StatusEventType.RENAME_ALL_VERSIONS, StatusEventType.UNDELETE_ALL_VERSIONS,
                StatusEventType.UNPUBLISH_ACCESS_GROUP, StatusEventType.UNPUBLISH_ALL_VERSIONS)) {
            final StoredStatusEvent setype = new StoredStatusEvent(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.READY, null, null);
            failConstructWithVersionLevelEvents(Arrays.asList(setype), new LinkedList<>(),
                    new IllegalArgumentException("Illegal initial event type: " + type));
            final StoredStatusEvent setype2 = new StoredStatusEvent(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.PROC, null, null);
            failConstructWithVersionLevelEvents(new LinkedList<>(), Arrays.asList(setype2),
                    new IllegalArgumentException("Illegal initial event type: " + type));
        }
    }
    
    private void failConstructWithVersionLevelEvents(
            final List<StoredStatusEvent> ready,
            final List<StoredStatusEvent> processing,
            final Exception expected) {
        try {
            new ObjectEventQueue(ready, processing);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void constructFailWithObjectLevelEvent() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        final StatusEventID id = new StatusEventID("some id");
        
        failConstructWithObjectLevelEvent(null, new NullPointerException("initialEvent"));
        
        // bad status
        for (final StatusEventProcessingState state: Arrays.asList(
                StatusEventProcessingState.FAIL, StatusEventProcessingState.INDX,
                StatusEventProcessingState.UNINDX, StatusEventProcessingState.UNPROC)) {
            failConstructWithObjectLevelEvent(new StoredStatusEvent(se, id, state, null, null),
                    new IllegalArgumentException("Illegal initial event state: " + state));
        }
        
        // bad event types
        for (final StatusEventType type: Arrays.asList(
                StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
                StatusEventType.NEW_VERSION, StatusEventType.PUBLISH_ACCESS_GROUP)) {
            final StoredStatusEvent setype = new StoredStatusEvent(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.READY, null, null);
            failConstructWithObjectLevelEvent(setype,
                    new IllegalArgumentException("Illegal initial event type: " + type));
        }
        
    }
    
    private void failConstructWithObjectLevelEvent(
            final StoredStatusEvent event,
            final Exception expected) {
        try {
            new ObjectEventQueue(event);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void loadFail() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        final StatusEventID id = new StatusEventID("some id");
        
        // null
        failLoad(null, new NullPointerException("event"));
        
        //bad state
        for (final StatusEventProcessingState state: Arrays.asList(
                StatusEventProcessingState.FAIL, StatusEventProcessingState.INDX,
                StatusEventProcessingState.UNINDX, StatusEventProcessingState.READY,
                StatusEventProcessingState.PROC)) {
            failLoad(new StoredStatusEvent(se, id, state, null, null),
                    new IllegalArgumentException("Illegal state for loading event: " + state));
        }
        
        // bad event types
        for (final StatusEventType type: Arrays.asList(
                StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
                StatusEventType.PUBLISH_ACCESS_GROUP)) {
            final StoredStatusEvent setype = new StoredStatusEvent(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.UNPROC, null, null);
            failLoad(setype,
                    new IllegalArgumentException("Illegal type for loading event: " + type));
        }
    }
    
    private void failLoad(final StoredStatusEvent sse, final Exception expected) {
        try {
            new ObjectEventQueue().load(sse);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
