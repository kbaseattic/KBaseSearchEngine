package kbasesearchengine.test.main;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Optional;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.test.common.TestCommon;

public class IndexerCoordinatorTest {

    @Test
    public void construct() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10);
        
        assertThat("incorrect queue size", coord.getMaximumQueueSize(), is(10));
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
    }
    
    @Test
    public void constructFailInputs() {
        final StatusEventStorage s = mock(StatusEventStorage.class);
        final LineLogger l = mock(LineLogger.class);
        
        failConstruct(null, l, 1, new NullPointerException("storage"));
        failConstruct(s, null, 1, new NullPointerException("logger"));
        failConstruct(s, l, 0,
                new IllegalArgumentException("maximumQueueSize must be at least 1"));
    }
    
    private void failConstruct(
            final StatusEventStorage storage,
            final LineLogger logger,
            final int maximumQueueSize,
            final Exception expected) {
        try {
            new IndexerCoordinator(storage, logger, maximumQueueSize);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void startIndexer() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor);
        
        coord.startIndexer();
        
        // we test that the runnable behaves as we expect in later tests
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void stopIndexer() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor);
        
        coord.stopIndexer();
        
        verify(executor).shutdown();
    }
    
    private Runnable getIndexerRunnable(
            final ScheduledExecutorService executorMock,
            final IndexerCoordinator coord) {
        final ArgumentCaptor<Runnable> indexerRunnable = ArgumentCaptor.forClass(Runnable.class);
        coord.startIndexer();
        verify(executorMock).scheduleAtFixedRate(
                indexerRunnable.capture(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
        return indexerRunnable.getValue();
    }
    
    // maybe make this an instance method that returns a new SSE
    // could take updater args as well
    private StoredStatusEvent to(
            final StoredStatusEvent sse,
            final StatusEventProcessingState state) {
        return new StoredStatusEvent(sse.getEvent(), sse.getId(), state,
                sse.getUpdateTime().orNull(), sse.getUpdater().orNull());
    }
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void blockingEvent() throws Exception {
        /* this test is a bit complex, since it runs through multiple indexer cycles
         * First call is two loops - first loop moves event1 to processing, second loop
         * gets no input and has no state change (since READY and PROC are treated the same)and so
         * exits
         * Second call is one loop - gets no input, just moves event1 out of the queue since it's
         * complete
         * Third call is one loop - gets no input, moves event2 into the queue and sets state
         * in storage
         */
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor);
        
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.UNPUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent event2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent ready1 = to(event1, StatusEventProcessingState.READY);
        final StoredStatusEvent proc1 = to(event1, StatusEventProcessingState.PROC);
        final StoredStatusEvent idx1 = to(event1, StatusEventProcessingState.INDX);
        final StoredStatusEvent ready2 = to(event1, StatusEventProcessingState.READY);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 10))
                .thenReturn(Arrays.asList(event1, event2))
                .thenReturn(Collections.emptyList());
        
        when(storage.get(new StatusEventID("foo1")))
                .thenReturn(Optional.of(ready1))
                .thenReturn(Optional.of(proc1)) // 2nd loop of 1st run call
                .thenReturn(Optional.of(idx1)); // this will return on the second run() call
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);

        coordRunner.run(); // this will move event1 out of the queue
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(storage, never()).setProcessingState(new StatusEventID("foo2"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        
        when(storage.get(new StatusEventID("foo2"))).thenReturn(Optional.of(ready2));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));

        verify(storage).setProcessingState(new StatusEventID("foo2"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
    }
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void eventLoadRequestSize() throws Exception {
        /* test that the coordinator requests the correct number of events from storage,
         * and that the coordinator stops cycling when no events were returned or the
         * queue is full
         */
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor);
        
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.UNPUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent event2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent event3 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(30000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC, null, null);
        
        final StoredStatusEvent ready1 = to(event1, StatusEventProcessingState.READY);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3))
                .thenReturn(Arrays.asList(event1))
                .thenReturn(null);
        when(storage.get(StatusEventProcessingState.UNPROC, 2))
                .thenReturn(Collections.emptyList()) //2nd loop of first call 
                .thenReturn(Arrays.asList(event2)) // second call
                .thenReturn(null);
        when(storage.get(StatusEventProcessingState.UNPROC, 1))
                .thenReturn(Collections.emptyList()) // 2nd loop of second call
                .thenReturn(Arrays.asList(event3)) // third call
                .thenReturn(null);
        
        when(storage.get(new StatusEventID("foo1")))
                .thenReturn(Optional.of(ready1)); //queue blocks forever
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(storage).get(StatusEventProcessingState.UNPROC, 2);
        verify(storage, never()).get(StatusEventProcessingState.UNPROC, 1);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        verify(storage).get(StatusEventProcessingState.UNPROC, 1);
        
        coordRunner.run();
        // will only cycle once because the queue is full
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(3));
        
        coordRunner.run();
        // should do nothing other than check state of event 1
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(3));

        verify(storage, times(6)).get(new StatusEventID("foo1"));
        verify(storage, never()).get(StatusEventProcessingState.UNPROC, 0);
        verify(storage, never()).setProcessingState(new StatusEventID("foo2"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(storage, never()).setProcessingState(new StatusEventID("foo3"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
    }
    
    @Test
    public void emptyNoInput() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3))
                .thenReturn(Collections.emptyList())
                .thenReturn(null); 
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(storage, never()).get(any());
        verify(storage, never()).get(any(), eq(2)); // must be an easier way of doing this
        verify(storage, never()).get(any(), eq(1));
        verify(storage, never()).get(any(), eq(0));
        verify(storage, never()).setProcessingState(any(), any(), any());
    }
    
    //TODO TEST startup with events
    
}
