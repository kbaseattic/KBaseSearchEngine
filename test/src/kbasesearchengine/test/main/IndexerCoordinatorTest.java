package kbasesearchengine.test.main;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Optional;
import com.google.common.base.Ticker;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.main.SignalMonitor;
import kbasesearchengine.test.common.TestCommon;

public class IndexerCoordinatorTest {
    
    private static final List<Integer> MT = Collections.emptyList();
    private static final Ticker ST = Ticker.systemTicker();
    private static final Clock SC = Clock.systemDefaultZone();
    // this will only work once
    private static final SignalMonitor SM = new SignalMonitor();

    @Test
    public void construct() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10);
        
        assertThat("incorrect max queue size", coord.getMaximumQueueSize(), is(10));
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
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 10, executor,
                MT, ST, SC);
        
        coord.startIndexer();
        
        // we test that the runnable behaves as we expect in later tests
        verify(executor).scheduleAtFixedRate(
                any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void stopIndexer() throws Exception {
        assertStopIndexerCorrect(0, 0);
    }
    
    @Test
    public void stopIndexerWithNegativeWaitTime() throws Exception {
        assertStopIndexerCorrect(-1, 0);
    }
    
    @Test
    public void stopIndexerWithLargeWaitTime() throws Exception {
        assertStopIndexerCorrect(100_000_000, 100_000_000);
    }

    private void assertStopIndexerCorrect(final int wait, final int expectedWait)
            throws InterruptedException, IndexingException {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 10, executor,
                MT, ST, SC);
        
        coord.stop(wait);
        
        verify(executor).shutdown();
        verify(executor).awaitTermination(expectedWait, TimeUnit.MILLISECONDS);
    }
    
    @Test
    public void stopIndexerWithContinousLoopRunning() throws Exception {
        /* tests the case when shutdown is called when the loop would normally continue running
         * because there's events flowing into the event storage
         */
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 10, executor,
                MT, ST, SC);
        
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.UNPUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final StoredStatusEvent ready1 = to(event1, StatusEventProcessingState.READY);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(eq(StatusEventProcessingState.UNPROC), anyInt()))
                .thenReturn(Arrays.asList(changeID(event1, "foo1")))
                .thenReturn(Arrays.asList(changeID(event1, "foo2")))
                .thenReturn(Collections.emptyList()) // end first cycle
                .thenAnswer(new Answer<List<StoredStatusEvent>>() {

                    @Override
                    public List<StoredStatusEvent> answer(final InvocationOnMock foo)
                            throws Throwable {
                        coord.stop(1000);
                        return Arrays.asList(changeID(event1, "foo3"));
                    }
                }) // cycle should end here because the coordinator has been stopped
                .thenReturn(Arrays.asList(changeID(event1, "foo4")));
        
        when(storage.get(new StatusEventID("foo1"))).thenReturn(Optional.of(ready1));
        when(storage.get(new StatusEventID("foo2")))
                .thenReturn(Optional.of(changeID(ready1, "foo2")));
        when(storage.get(new StatusEventID("foo3")))
                .thenReturn(Optional.of(changeID(ready1, "foo3")));

        coordRunner.run();
        // changed when fast loop behavior removed 18/2/21
//      assertThat("incorrect cycle count", coord.getContinuousCycles(), is(3));
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        coordRunner.run(); //added 18/2/21
        coordRunner.run(); //added 18/2/21
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        coordRunner.run(); // stop is called on first cycle
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(3));
        
        coordRunner.run(); // indexer is stopped
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(3));
        
        verify(storage, never()).get(new StatusEventID("foo4"));
    }
    
    private StoredStatusEvent changeID(final StoredStatusEvent event, final String id) {
        return StoredStatusEvent.getBuilder(event.getEvent(), new StatusEventID(id),
                event.getState())
                .withNullableUpdate(event.getUpdateTime().orNull(), event.getUpdater().orNull())
                .build();
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
        return to(sse, state, null);
    }
    
    private StoredStatusEvent to(
            final StoredStatusEvent sse,
            final StatusEventProcessingState state,
            final String updater) {
        return StoredStatusEvent.getBuilder(sse.getEvent(), sse.getID(), state)
                .withNullableUpdate(
                        Instant.now(), updater == null ? sse.getUpdater().orNull() : updater)
                .build();
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
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
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 10, executor,
                MT, ST, SC);
        
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.UNPUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final StoredStatusEvent event2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC).build();
        
        final StoredStatusEvent ready1 = to(event1, StatusEventProcessingState.READY);
        final StoredStatusEvent proc1 = to(ready1, StatusEventProcessingState.PROC, "work1");
        final StoredStatusEvent idx1 = to(proc1, StatusEventProcessingState.INDX);
        final StoredStatusEvent ready2 = to(event2, StatusEventProcessingState.READY);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 10))
                .thenReturn(Arrays.asList(event1, event2))
                .thenReturn(Collections.emptyList());
        
        when(storage.get(new StatusEventID("foo1")))
                .thenReturn(Optional.of(ready1))
                .thenReturn(Optional.of(proc1)) // 2nd loop of 1st run call [18/2/21: 2nd run call]
                .thenReturn(Optional.of(idx1)); // this will return on the second run() call [18/2/21: 3rd]
        
        coordRunner.run();
        // changed when fast loop behavior removed 18/2/21
//        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(logger).logInfo(
                "Moved event foo1 UNPUBLISH_ACCESS_GROUP WS:2/null from UNPROC to READY");

        coordRunner.run(); // returns that event is ready 18/2/21
        coordRunner.run(); // this will move event1 out of the queue
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(logger).logInfo("Event foo1 UNPUBLISH_ACCESS_GROUP WS:2/null completed " +
                "processing with state INDX on worker work1");
        
        verify(storage, never()).setProcessingState(new StatusEventID("foo2"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        
        when(storage.get(new StatusEventID("foo2"))).thenReturn(Optional.of(ready2));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));

        verify(storage).setProcessingState(new StatusEventID("foo2"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(logger).logInfo(
                "Moved event foo2 PUBLISH_ACCESS_GROUP WS:2/null from UNPROC to READY");
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
    public void eventLoadRequestSize() throws Exception {
        /* test that the coordinator requests the correct number of events from storage,
         * and that the coordinator stops cycling when no events were returned or the
         * queue is full
         */
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, SC);
        
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.UNPUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final StoredStatusEvent event2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.UNPROC).build();
        
        final StoredStatusEvent event3 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(30000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo3"), StatusEventProcessingState.UNPROC).build();
        
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
        // changed when fast loop behavior removed 18/2/21
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
//        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(logger).logInfo(
                "Moved event foo1 UNPUBLISH_ACCESS_GROUP WS:2/null from UNPROC to READY");
        
        coordRunner.run(); // added 18/2/21
        
        verify(storage).get(StatusEventProcessingState.UNPROC, 2);
        verify(storage, never()).get(StatusEventProcessingState.UNPROC, 1);
        
        coordRunner.run();
     // changed when fast loop behavior removed 18/2/21
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
//        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        coordRunner.run(); // added 18/2/21
        
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
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
    public void emptyNoInput() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, SC);
        
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
        verify(logger, never()).logInfo(any());
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
    public void constructWithMultipleEvents() throws Exception {
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("1")
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY)
                .withNullableUpdate(Instant.now(), null)
                .build();
        
        final StoredStatusEvent event2 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("2")
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.PROC)
                .withNullableUpdate(Instant.now(), null)
                .build();
        
        final StoredStatusEvent unidx1 = to(event1, StatusEventProcessingState.UNINDX, "work1");
        final StoredStatusEvent fail2 = to(event2, StatusEventProcessingState.FAIL, "work2");
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        when(storage.get(StatusEventProcessingState.READY, 3)).thenReturn(Arrays.asList(event1));
        when(storage.get(StatusEventProcessingState.PROC, 3)).thenReturn(Arrays.asList(event2));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, SC);
        assertThat("incorrect queue size", coord.getQueueSize(), is(2));
        
        when(storage.get(eq(StatusEventProcessingState.UNPROC), anyInt()))
                .thenReturn(Collections.emptyList());
        when(storage.get(new StatusEventID("foo1")))
                .thenReturn(Optional.of(event1))
                .thenReturn(Optional.of(unidx1)) //2nd call
                .thenReturn(null);
        when(storage.get(new StatusEventID("foo2")))
                .thenReturn(Optional.of(fail2))
                .thenReturn(null);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger).logInfo("Event foo2 RENAME_ALL_VERSIONS WS:2/2 completed " +
                "processing with state FAIL on worker work2");
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        verify(logger).logInfo("Event foo1 PUBLISH_ALL_VERSIONS WS:2/1 completed " +
                "processing with state UNINDX on worker work1");
        
        verify(storage, never()).setProcessingState(any(), any(), any());
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
    public void constructWithSingleEvent() throws Exception {
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY)
                .withNullableUpdate(Instant.now(), null)
                .build();
        
        final StoredStatusEvent idx1 = to(event1, StatusEventProcessingState.INDX, "work1");
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        when(storage.get(StatusEventProcessingState.READY, 3)).thenReturn(Arrays.asList(event1));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, SC);
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        when(storage.get(eq(StatusEventProcessingState.UNPROC), anyInt()))
                .thenReturn(Collections.emptyList());
        when(storage.get(new StatusEventID("foo1")))
                .thenReturn(Optional.of(event1))
                .thenReturn(Optional.of(idx1)) //2nd call
                .thenReturn(null);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger, never()).logInfo(any());
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null completed " +
                "processing with state INDX on worker work1");
        
        verify(storage, never()).setProcessingState(any(), any(), any());
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test(timeout = 2000) // in case the coordinator loops forever
    public void desynchedQueue() throws Exception {
        /* test the case where the in memory queue does not match the DB. */
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, SC);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3))
                .thenReturn(Arrays.asList(event1))
                .thenReturn(Collections.emptyList());
        
        when(storage.get(new StatusEventID("foo1"))).thenReturn(Optional.absent());
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        // changed when fast loop behavior removed 18/2/21
//      assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(logger).logInfo(
                "Moved event foo1 PUBLISH_ACCESS_GROUP WS:2/null from UNPROC to READY");
        
        verify(logger).logError("Event foo1 is in the in-memory queue but not " +
                            "in the storage system. Removing from queue");
    }
    
    @Test
    public void fatalErrorOnGetUnprocessed() throws Exception {
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final SignalMonitor sm = mock(SignalMonitor.class);

        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, sm, 3, executor,
                Arrays.asList(1, 1), ST, SC);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenThrow(
                new FatalRetriableIndexingException(ErrorType.OTHER, "wheee!"));
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(executor).shutdown();
        verify(sm).signal();

        verify(logger).logError("Retriable error in indexer, retry 1: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: wheee!");
        verify(logger).logError("Retriable error in indexer, retry 2: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: wheee!");
        verify(logger).logError("Fatal error in indexer, shutting down: " +
                "kbasesearchengine.events.exceptions.FatalIndexingException: wheee!");
        
        final ArgumentCaptor<Exception> retExpCaptor = 
                ArgumentCaptor.forClass(FatalRetriableIndexingException.class);
        verify(logger, times(3)).logError(retExpCaptor.capture());
        
        final List<Exception> expected = Arrays.asList(
                new FatalRetriableIndexingException(ErrorType.OTHER, "wheee!"),
                new FatalRetriableIndexingException(ErrorType.OTHER, "wheee!"),
                new FatalIndexingException(ErrorType.OTHER, "wheee!"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    @Test
    public void unexpectedErrorOnGetUnprocessed() throws Exception {
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final SignalMonitor sm = mock(SignalMonitor.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, sm, 3, executor,
                Arrays.asList(1, 1), ST, SC);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenThrow(
                new RuntimeException("arg"));
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));

        verify(executor, never()).shutdown();
        verify(sm, never()).signal();
        
        verify(logger).logError("Unexpected error in indexer: java.lang.RuntimeException: arg");
        
        final ArgumentCaptor<Exception> retExpCaptor = 
                ArgumentCaptor.forClass(FatalRetriableIndexingException.class);
        verify(logger).logError(retExpCaptor.capture());
        
        final List<Exception> expected = Arrays.asList(
                new RuntimeException("arg"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    @Test
    public void fatalErrorOnSetState() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final SignalMonitor sm = mock(SignalMonitor.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, sm, 3, executor,
                Arrays.asList(1), ST, SC);
        
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenReturn(Arrays.asList(event1));
        
        when(storage.setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY)).thenThrow(
                        new FatalRetriableIndexingException(
                                ErrorType.OTHER, "oof ouch owie my bones"));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(executor).shutdown();
        verify(sm).signal();

        verify(logger).logError("Retriable error in indexer for event " +
                "PUBLISH_ACCESS_GROUP foo1, retry 1: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: " +
                "oof ouch owie my bones");
        verify(logger).logError("Fatal error in indexer, shutting down: " +
                "kbasesearchengine.events.exceptions.FatalIndexingException: " +
                "oof ouch owie my bones");
        
        final ArgumentCaptor<Exception> retExpCaptor = 
                ArgumentCaptor.forClass(FatalRetriableIndexingException.class);
        verify(logger, times(2)).logError(retExpCaptor.capture());
        
        final List<Exception> expected = Arrays.asList(
                new FatalRetriableIndexingException(ErrorType.OTHER, "oof ouch owie my bones"),
                new FatalIndexingException(ErrorType.OTHER, "oof ouch owie my bones"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    @Test
    public void fatalErrorOnGet() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final SignalMonitor sm = mock(SignalMonitor.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, sm, 3, executor,
                Arrays.asList(1, 1, 1), ST, SC);
        
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("1")
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC).build();
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenReturn(Arrays.asList(event1));
        
        when(storage.get(new StatusEventID("foo1"))).thenThrow(
                        new FatalRetriableIndexingException(ErrorType.OTHER, "yay"));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(executor).shutdown();
        verify(sm).signal();
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        verify(logger).logInfo(
                "Moved event foo1 DELETE_ALL_VERSIONS WS:2/1 from UNPROC to READY");
        verify(executor).shutdown();

        verify(logger).logError("Retriable error in indexer for event " +
                "DELETE_ALL_VERSIONS foo1, retry 1: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: yay");
        verify(logger).logError("Retriable error in indexer for event " +
                "DELETE_ALL_VERSIONS foo1, retry 2: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: yay");
        verify(logger).logError("Retriable error in indexer for event " +
                "DELETE_ALL_VERSIONS foo1, retry 3: " +
                "kbasesearchengine.events.exceptions.FatalRetriableIndexingException: yay");
        verify(logger).logError("Fatal error in indexer, shutting down: " +
                "kbasesearchengine.events.exceptions.FatalIndexingException: yay");
        
        final ArgumentCaptor<Exception> retExpCaptor = 
                ArgumentCaptor.forClass(FatalRetriableIndexingException.class);
        verify(logger, times(4)).logError(retExpCaptor.capture());
        
        final List<Exception> expected = Arrays.asList(
                new FatalRetriableIndexingException(ErrorType.OTHER, "yay"),
                new FatalRetriableIndexingException(ErrorType.OTHER, "yay"),
                new FatalRetriableIndexingException(ErrorType.OTHER, "yay"),
                new FatalIndexingException(ErrorType.OTHER, "yay"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    @Test
    public void logDelayed() throws Exception {
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.PROC)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "work1")
                .build();
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final Clock clock = mock(Clock.class);
        
        when(storage.get(StatusEventProcessingState.READY, 3)).thenReturn(Arrays.asList(event1));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ST, clock);
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));

        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(clock.instant())
                .thenReturn(Instant.ofEpochMilli(10000 + (3600 * 1000)))
                .thenReturn(Instant.ofEpochMilli(10000 + (3600 * 1000) + 1000))
                .thenReturn(Instant.ofEpochMilli(10000 + (2 * 3600 * 1000) + 1000))
                .thenReturn(Instant.ofEpochMilli(10000 + (2 * 3600 * 1000) + 2000))
                .thenReturn(null);
        
        when(storage.get(new StatusEventID("foo1"))).thenReturn(Optional.of(event1));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger, never()).logInfo(any());
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state PROC " +
                "has been processing for 1 hours on worker work1");
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger, never()).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state " +
                "PROC has been processing for 2 hours on worker work1");
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state PROC " +
                "has been processing for 2 hours on worker work1");

        verify(storage, never()).setProcessingState(any(), any(), any());
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test
    public void logDelayedWithCacheExpiry() throws Exception {
        final StoredStatusEvent event1 = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY)
                .withNullableUpdate(Instant.ofEpochMilli(10000), "work1")
                .build();
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final Clock clock = mock(Clock.class);
        final Ticker ticker = mock(Ticker.class);
        
        when(storage.get(StatusEventProcessingState.PROC, 3)).thenReturn(Arrays.asList(event1));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, SM, 3, executor,
                MT, ticker, clock);
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));

        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(clock.instant())
                .thenReturn(Instant.ofEpochMilli(10000 + (3600 * 1000) + 1000))
                .thenReturn(Instant.ofEpochMilli(10000 + (2 * 3600 * 1000))) // cache prevents log
                .thenReturn(Instant.ofEpochMilli(10000 + (2 * 3600 * 1000))) // cache expires
                .thenReturn(null);
        
        when(ticker.read())
                .thenReturn(0L) // 1st run put
                .thenReturn(3599 * 1_000_000_000L) // 2nd run get, no put
                .thenReturn(3 * 3600 * 1_000_000_000L) //3rd run get force expiration
                .thenReturn(1_000_000_000_000_000_000L); // 3rd run put

        when(storage.get(new StatusEventID("foo1"))).thenReturn(Optional.of(event1));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state READY " +
                "has been processing for 1 hours on worker work1");
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        // this verifies the *previous* log. Would need to add times(2) to verify another log.
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state READY " +
                "has been processing for 1 hours on worker work1");
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        // this verifies the *previous* log. Would need to add times(2) to verify another log.
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state READY " +
                "has been processing for 1 hours on worker work1");
        verify(logger).logInfo("Event foo1 PUBLISH_ACCESS_GROUP WS:2/null in state READY " +
                "has been processing for 2 hours on worker work1");
        
        verify(storage, never()).setProcessingState(any(), any(), any());
        verify(logger, never()).logError(any(String.class));
        verify(logger, never()).logError(any(Throwable.class));
    }
    
    @Test
    public void awaitShutdown() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final SignalMonitor sm = new SignalMonitor();
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, sm, 3, executor,
                Arrays.asList(1), ST, SC);
        
        final Thread shutdownThread = new Thread() {
            
            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Test borked", e);
                }
                sm.signal();
            }
        };
        
        final Instant now = Instant.now();
        shutdownThread.start();
        coord.awaitShutdown();
        TestCommon.assertCloseMS(now, Instant.now(), 200, 50);
    }
}
