package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Set;

import static kbasesearchengine.test.common.TestCommon.set;

import org.junit.Test;

import kbasesearchengine.events.ObjectEventQueue;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;

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
    public void initializeWithReadyObjectLevelEvent() {
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
    public void initializeWithProcessingObjectLevelEvent() {
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
    public void initializeWithVersionLevelEvents() {
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
    
    //TODO TEST returned sets are immutable
}
