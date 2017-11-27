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

import kbasesearchengine.events.EventQueue;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.test.common.TestCommon;

public class EventQueueTest {
    
    /* this assert does not mutate the queue state */
    private void assertQueueState(
            final EventQueue queue,
            final Set<StoredStatusEvent> ready,
            final Set<StoredStatusEvent> processing,
            final int size) {
        assertThat("incorrect ready", queue.getReadyForProcessing(), is(ready));
        assertThat("incorrect get processing", queue.getProcessing(), is(processing));
        assertThat("incorrect size", queue.size(), is(size));
        assertThat("incorrect isEmpty", queue.isEmpty(), is(size == 0));
    }
    
    /* this assert does not mutate the queue state */
    private void assertEmpty(final EventQueue queue) {
        assertQueueState(queue, set(), set(), 0);
        assertMoveToReadyCorrect(queue, set());
        assertMoveToProcessingCorrect(queue, set());
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToProcessingCorrect(
            final EventQueue queue,
            final Set<StoredStatusEvent> moveToProcessing) {
        assertThat("incorrect move", queue.moveReadyToProcessing(), is(moveToProcessing));
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToReadyCorrect(
            final EventQueue queue,
            final Set<StoredStatusEvent> moveToReady) {
        assertThat("incorrect move", queue.moveToReady(), is(moveToReady));
    }
    
    private StoredStatusEvent createEvent(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final StatusEventType type,
            final StatusEventProcessingState state,
            final String objectID) {
        return new StoredStatusEvent(StatusEvent.getBuilder(
                "storagecode", time, type)
                .withNullableObjectID(objectID)
                .withNullableAccessGroupID(accgrpID)
                .build(),
                new StatusEventID(eventid), state, null, null);
    }
    
    private StoredStatusEvent unproc(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(accgrpID, eventid, time, type, StatusEventProcessingState.UNPROC, objectID);
    }
    
    private StoredStatusEvent ready(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(accgrpID, eventid, time, type, StatusEventProcessingState.READY, objectID);
    }
    
    private StoredStatusEvent proc(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        return createEvent(accgrpID, eventid, time, type, StatusEventProcessingState.PROC, objectID);
    }
    
    private StoredStatusEvent readyVer(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID) {
        return createEvent(accgrpID, eventid, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.READY, objectID);
    }
    
    private StoredStatusEvent procVer(
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID) {
        return createEvent(accgrpID, eventid, time, StatusEventType.NEW_VERSION,
                StatusEventProcessingState.PROC, objectID);
    }
    
    private StoredStatusEvent loadUnproc(
            final EventQueue queue,
            final int accgrpID,
            final String eventid,
            final Instant time,
            final String objectID,
            final StatusEventType type) {
        final StoredStatusEvent e = unproc(accgrpID, eventid, time, objectID, type);
        queue.load(e);
        return e;
    }
    
    @Test
    public void constructEmpty() {
        assertEmpty(new EventQueue());
        assertEmpty(new EventQueue(Collections.emptyList()));
    }
    
    @Test
    public void moveEventsThroughQueue() {
        final EventQueue q = new EventQueue();
        
        assertEmpty(q);
        
        final StoredStatusEvent e1 = loadUnproc(
                q, 1, "1", Instant.ofEpochMilli(10000), "1", StatusEventType.DELETE_ALL_VERSIONS);
        assertQueueState(q, set(), set(), 1);
        final StoredStatusEvent e2 = loadUnproc(
                q, 1, "2", Instant.ofEpochMilli(10000), "3", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e3 = loadUnproc(
                q, 1, "3", Instant.ofEpochMilli(15000), "3", StatusEventType.RENAME_ALL_VERSIONS);
        final StoredStatusEvent e4 = loadUnproc(
                q, 2, "4", Instant.ofEpochMilli(15000), "1", StatusEventType.COPY_ACCESS_GROUP);
        assertQueueState(q, set(), set(), 4);
        
        assertMoveToReadyCorrect(q, set(e1, e2, e4));
        assertQueueState(q, set(e1, e2, e4), set(), 4);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set(e1, e2, e4));
        assertQueueState(q, set(), set(e1, e2, e4), 4);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e4);
        assertQueueState(q, set(), set(e1, e2), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e2);
        assertQueueState(q, set(e3), set(e1), 2);
        assertMoveToReadyCorrect(q, set());
        q.setProcessingComplete(e1);
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
    public void loadFail() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableAccessGroupID(1).build();
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
    
    private void failLoad(final StoredStatusEvent event, final Exception expected) {
        try {
            new EventQueue().load(event);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void setProcessingCompleteFail() {
        final EventQueue q = new EventQueue();
        final StoredStatusEvent sse = new StoredStatusEvent(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("id")
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY, null, null);
        
        //nulls
        failSetProcessingComplete(q, null, new NullPointerException("event"));
        
        //empty queue
        failSetProcessingComplete(q, sse, new NoSuchEventException(sse));
        
        // with group level event in processed state with different event id
        final EventQueue q2 = new EventQueue(Arrays.asList(
                new StoredStatusEvent(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ACCESS_GROUP)
                        .withNullableAccessGroupID(1)
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC, null, null)));
        failSetProcessingComplete(q2, sse, new NoSuchEventException(sse));
        
        // with group level event with different group id
        final EventQueue q3 = new EventQueue(Arrays.asList(
                new StoredStatusEvent(StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                        .withNullableAccessGroupID(2)
                        .withNullableObjectID("id")
                        .build(),
                        new StatusEventID("foo"), StatusEventProcessingState.PROC, null, null)));
        failSetProcessingComplete(q3, sse, new NoSuchEventException(sse));
        
    }
    
    private void failSetProcessingComplete(
            final EventQueue queue,
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
    public void constructWithEventsAndMoveThroughQueue() {
        final StoredStatusEvent e1 = proc(
                1, "1", Instant.ofEpochMilli(10000), "1", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent e2 = readyVer(
                1, "2", Instant.ofEpochMilli(10000), "3");
        final StoredStatusEvent e3 = procVer(
                1, "3", Instant.ofEpochMilli(15000), "3");
        final StoredStatusEvent e4 = ready(
                2, "4", Instant.ofEpochMilli(15000), "1", StatusEventType.COPY_ACCESS_GROUP);

        final EventQueue q = new EventQueue(Arrays.asList(e1, e2, e3, e4));
        
        assertQueueState(q, set(e2, e4), set(e1, e3), 4);
        
        assertMoveToReadyCorrect(q, set());
        
        q.setProcessingComplete(e3);
        assertQueueState(q, set(e2, e4), set(e1), 3);
        assertMoveToReadyCorrect(q, set());
        
        assertMoveToProcessingCorrect(q, set(e2, e4));
        assertQueueState(q, set(), set(e1, e2, e4), 3);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e4);
        assertQueueState(q, set(), set(e1, e2), 2);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        
        q.setProcessingComplete(e2);
        assertQueueState(q, set(), set(e1), 1);
        assertMoveToReadyCorrect(q, set());
        assertMoveToProcessingCorrect(q, set());
        q.setProcessingComplete(e1);
        
        assertEmpty(q);
    }
    
    @Test
    public void constructFail() {
        final StoredStatusEvent p = proc(
                1, "1", Instant.ofEpochMilli(10000), "1", StatusEventType.DELETE_ALL_VERSIONS);
        final StoredStatusEvent u = unproc(
                1, "1", Instant.ofEpochMilli(10000), "1", StatusEventType.DELETE_ALL_VERSIONS);
        
        //nulls
        failConstruct(null, new NullPointerException("initialLoad"));
        failConstruct(Arrays.asList(p, null),
                new NullPointerException("initialLoad has null entries"));
        
        // error thrown by composed AccessGroupQ
        failConstruct(Arrays.asList(u),
                new IllegalArgumentException("Illegal initial event state: UNPROC"));
    }
    
    private void failConstruct(final List<StoredStatusEvent> events, final Exception expected) {
        try {
            new EventQueue(events);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
