package kbasesearchengine.test.events;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.util.Set;

import org.junit.Test;

import kbasesearchengine.events.AccessGroupEventQueue;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;

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

    //TODO QUEUE NOW constructor tests
    //TODO QUEUE NOW unhappy path tests
    //TODO QUEUE NOW immutable results tests
    
}
