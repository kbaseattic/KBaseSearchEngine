package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.events.ObjectEventQueue;
import kbasesearchengine.events.exceptions.NoSuchEventException;
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
            final Optional<StoredStatusEvent> ready,
            final Optional<StoredStatusEvent> processing,
            final int size) {
        assertThat("incorrect ready", queue.getReadyForProcessing(), is(ready));
        assertThat("incorrect hasReady", queue.hasReady(), is(ready.isPresent()));
        assertThat("incorrect get processing", queue.getProcessing(), is(processing));
        assertThat("incorrect is processing", queue.isProcessing(), is(processing.isPresent()));
        assertThat("incorrect is proc or ready", queue.isProcessingOrReady(),
                is(processing.isPresent() || ready.isPresent()));
        assertThat("incorrect size", queue.size(), is(size));
        assertThat("incorrect isEmpty", queue.isEmpty(), is(size == 0));
    }
    
    /* this assert does not mutate the queue state */
    private void assertEmpty(final ObjectEventQueue queue) {
        assertQueueState(queue, Optional.absent(), Optional.absent(), 0);
        assertMoveToReadyCorrect(queue, Optional.absent());
        assertMoveToProcessingCorrect(queue, Optional.absent());
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToProcessingCorrect(
            final ObjectEventQueue queue,
            final Optional<StoredStatusEvent> moveToProcessing) {
        assertThat("incorrect move", queue.moveReadyToProcessing(), is(moveToProcessing));
    }
    
    /* note this assert may change the queue state. */
    private void assertMoveToReadyCorrect(
            final ObjectEventQueue queue,
            final Optional<StoredStatusEvent> moveToReady) {
        assertThat("incorrect move", queue.moveToReady(), is(moveToReady));
    }
    
    @Test
    public void constructEmpty() {
        assertEmpty(new ObjectEventQueue());
    }
    
    @Test
    public void loadOneEventAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse = Optional.of(sse);
        
        
        assertEmpty(q);
        
        q.load(sse);
        assertQueueState(q, Optional.absent(), Optional.absent(), 1);
        assertMoveToReadyCorrect(q, osse); //mutates queue
        assertQueueState(q, osse, Optional.absent(), 1);
        assertMoveToProcessingCorrect(q, osse); //mutates queue
        assertQueueState(q, Optional.absent(), osse, 1);
        
        q.setProcessingComplete(sse);
        assertEmpty(q);
    }
    
    @Test
    public void loadMultipleEventsAndProcess() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse = Optional.of(sse);
        final StoredStatusEvent sse1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse1 = Optional.of(sse1);
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.RENAME_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse2 = Optional.of(sse2);
        
        assertEmpty(q);
        
        q.load(sse2);
        q.load(sse1);
        q.load(sse);
        assertQueueState(q, Optional.absent(), Optional.absent(), 3);
        assertMoveToReadyCorrect(q, osse);
        assertQueueState(q, osse, Optional.absent(), 3);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, osse);
        assertQueueState(q, Optional.absent(), osse, 3);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, Optional.absent());
        
        q.setProcessingComplete(sse); // calls move to ready
        assertQueueState(q, osse1, Optional.absent(), 2);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, osse1);
        assertQueueState(q, Optional.absent(), osse1, 2);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, Optional.absent());
        
        q.setProcessingComplete(sse1);
        assertQueueState(q, osse2, Optional.absent(), 1);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, osse2);
        assertQueueState(q, Optional.absent(), osse2, 1);
        //check queue is blocked
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, Optional.absent());
        
        q.setProcessingComplete(sse2);
        assertEmpty(q);
    }
    
    @Test
    public void constructWithReadyObjectLevelEvent() {
        for (final StatusEventType type: Arrays.asList(StatusEventType.DELETE_ALL_VERSIONS,
                StatusEventType.NEW_ALL_VERSIONS, StatusEventType.PUBLISH_ALL_VERSIONS,
                StatusEventType.RENAME_ALL_VERSIONS, StatusEventType.UNDELETE_ALL_VERSIONS,
                StatusEventType.NEW_VERSION)) {
            assertSingleObjectLevelReadyEvent(type);
            
        }
    }

    private void assertSingleObjectLevelReadyEvent(final StatusEventType type) {
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), type)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertQueueState(q, Optional.of(sse), Optional.absent(), 1);
    }
    
    @Test
    public void constructWithProcessingObjectLevelEvent() {
        for (final StatusEventType type: Arrays.asList(StatusEventType.DELETE_ALL_VERSIONS,
                StatusEventType.NEW_ALL_VERSIONS, StatusEventType.PUBLISH_ALL_VERSIONS,
                StatusEventType.RENAME_ALL_VERSIONS, StatusEventType.UNDELETE_ALL_VERSIONS,
                StatusEventType.NEW_VERSION)) {
            assertSingleObjectLevelProcessingEvent(type);
            
        }
    }
    
    private void assertSingleObjectLevelProcessingEvent(final StatusEventType type) {
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), type)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.PROC).build();
        final ObjectEventQueue q = new ObjectEventQueue(sse);
        assertQueueState(q, Optional.absent(), Optional.of(sse), 1);
    }
    
    
    @Test
    public void setProcessedWithMutatedEvent() {
        // in practice we expect the events passed into setProcessed() to have mutated slightly
        // from the original load()ed event, so check that works.
        // the status event itself and the id should not mutate, but other fields are fair game.
        
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        
        final ObjectEventQueue q = new ObjectEventQueue();
        q.load(sse);
        q.moveToReady();
        q.moveReadyToProcessing();
        assertQueueState(q, Optional.absent(), Optional.of(sse), 1);
        
        final StoredStatusEvent hideousmutant = StoredStatusEvent.getBuilder(
                StatusEvent.getBuilder("bar", Instant.ofEpochMilli(10000),
                        StatusEventType.NEW_VERSION).build(),
                new StatusEventID("foo"), StatusEventProcessingState.INDX)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "whee")
                .build();
        
        q.setProcessingComplete(hideousmutant);
        assertEmpty(q);
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
            failConstructWithObjectLevelEvent(StoredStatusEvent.getBuilder(se, id, state).build(),
                    new IllegalArgumentException("Illegal initial event state: " + state));
        }
        
        // bad event types
        for (final StatusEventType type: Arrays.asList(
                StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
                StatusEventType.PUBLISH_ACCESS_GROUP)) {
            final StoredStatusEvent setype = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.READY).build();
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
            failLoad(StoredStatusEvent.getBuilder(se, id, state).build(),
                    new IllegalArgumentException("Illegal state for loading event: " + state));
        }
        
        // bad event types
        for (final StatusEventType type: Arrays.asList(
                StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
                StatusEventType.PUBLISH_ACCESS_GROUP)) {
            final StoredStatusEvent setype = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                    "foo", Instant.ofEpochMilli(10000), type).build(),
                    id, StatusEventProcessingState.UNPROC).build();
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
    
    @Test
    public void setProcessingCompleteFail() {
        final ObjectEventQueue q = new ObjectEventQueue();
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        
        //nulls
        failSetProcessingComplete(q, null, new NullPointerException("event"));
        
        //empty queue
        failSetProcessingComplete(q, sse, new NoSuchEventException(sse));
        
        // with object level event in processed state
        final ObjectEventQueue q2 = new ObjectEventQueue(StoredStatusEvent.getBuilder(
                StatusEvent.getBuilder(
                        "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                        .build(),
                        new StatusEventID("foo2"), StatusEventProcessingState.PROC).build());
        failSetProcessingComplete(q2, sse, new NoSuchEventException(sse));
    }
    
    private void failSetProcessingComplete(
            final ObjectEventQueue queue,
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
    public void setGetAndRemoveBlock() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        assertThat("incorrect block time", q.getBlockTime(), is(Optional.absent()));
        q.removeBlock(); // noop
        assertThat("incorrect block time", q.getBlockTime(), is(Optional.absent()));
        
        q.drainAndBlockAt(Instant.ofEpochMilli(10000));
        assertThat("incorrect block time", q.getBlockTime(),
                is(Optional.of(Instant.ofEpochMilli(10000))));
        
        q.removeBlock();
        assertThat("incorrect block time", q.getBlockTime(), is(Optional.absent()));
    }
    
    @Test
    public void drainAndBlockAtFail() {
        final ObjectEventQueue q = new ObjectEventQueue();
        try {
            q.drainAndBlockAt(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("blockTime"));
        }
    }
    
    @Test
    public void drainAndBlockEventsWithEventInReadyAndNoDrain() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse = Optional.of(sse);
        final StoredStatusEvent sse1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_VERSION)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse1 = Optional.of(sse1);
        
        assertEmpty(q);
        
        q.load(sse);
        q.load(sse1);
        assertMoveToReadyCorrect(q, osse);
        assertQueueState(q, osse, Optional.absent(), 2);
        q.drainAndBlockAt(Instant.ofEpochMilli(5000));
        assertMoveToReadyCorrect(q, Optional.absent());
        assertQueueState(q, osse, Optional.absent(), 2);
        assertMoveToProcessingCorrect(q, osse);
        assertQueueState(q, Optional.absent(), osse, 2);
        q.setProcessingComplete(sse); // if not blocked would move sse1 into position
        assertQueueState(q, Optional.absent(), Optional.absent(), 1);
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, Optional.absent());
        assertQueueState(q, Optional.absent(), Optional.absent(), 1);
        
        q.removeBlock();
        assertMoveToReadyCorrect(q, osse1);
        assertQueueState(q, osse1, Optional.absent(), 1);
    }
    
    @Test
    public void drainAndBlockEvents() {
        final ObjectEventQueue q = new ObjectEventQueue();
        
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse = Optional.of(sse);
        final StoredStatusEvent sse1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar1", Instant.ofEpochMilli(20000), StatusEventType.NEW_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse1 = Optional.of(sse1);
        final StoredStatusEvent sse2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "bar2", Instant.ofEpochMilli(30000), StatusEventType.RENAME_ALL_VERSIONS)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC).build();
        final Optional<StoredStatusEvent> osse2 = Optional.of(sse2);
        
        assertEmpty(q);
        
        q.load(sse2);
        q.load(sse1);
        q.load(sse);
        
        q.drainAndBlockAt(Instant.ofEpochMilli(25000));
        
        assertMoveToReadyCorrect(q, osse);
        assertMoveToProcessingCorrect(q, osse);
        assertQueueState(q, Optional.absent(), osse, 3);
        q.setProcessingComplete(sse); // moves next to ready
        assertQueueState(q, osse1, Optional.absent(), 2);
        
        assertMoveToReadyCorrect(q, Optional.absent());
        assertMoveToProcessingCorrect(q, osse1);
        q.setProcessingComplete(sse1);
        assertQueueState(q, Optional.absent(), Optional.absent(), 1);
        
        assertMoveToReadyCorrect(q, Optional.absent()); // queue blocked
        assertMoveToProcessingCorrect(q, Optional.absent());
        
        q.removeBlock();
        assertMoveToReadyCorrect(q, osse2);
        assertMoveToProcessingCorrect(q, osse2);
        q.setProcessingComplete(sse2);
        assertEmpty(q);
    }
}
