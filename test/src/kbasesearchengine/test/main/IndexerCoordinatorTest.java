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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.test.common.TestCommon;

public class IndexerCoordinatorTest {
    
    private static final List<Integer> MT = Collections.emptyList();

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
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor, MT);
        
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
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor, MT);
        
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
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 10, executor, MT);
        
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
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor, MT);
        
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
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void emptyNoInput() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor, MT);
        
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
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void constructWithMultipleEvents() throws Exception {
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("1")
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY, null, null);
        
        final StoredStatusEvent event2 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(20000), StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("2")
                .build(),
                new StatusEventID("foo2"), StatusEventProcessingState.PROC, null, null);
        
        final StoredStatusEvent unidx1 = to(event1, StatusEventProcessingState.UNINDX);
        final StoredStatusEvent fail2 = to(event2, StatusEventProcessingState.FAIL);
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        when(storage.get(StatusEventProcessingState.READY, 3)).thenReturn(Arrays.asList(event1));
        when(storage.get(StatusEventProcessingState.PROC, 3)).thenReturn(Arrays.asList(event2));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor, MT);
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
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(storage, never()).setProcessingState(any(), any(), any());
    }
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void constructWithSingleEvent() throws Exception {
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.READY, null, null);
        
        final StoredStatusEvent idx1 = to(event1, StatusEventProcessingState.INDX);
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        when(storage.get(StatusEventProcessingState.READY, 3)).thenReturn(Arrays.asList(event1));
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor, MT);
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
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(1));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(storage, never()).setProcessingState(any(), any(), any());
    }
    
    @Test(timeout = 1000) // in case the coordinator loops forever
    public void desynchedQueue() throws Exception {
        /* test the case where the in memory queue does not match the DB. */
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor, MT);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3))
                .thenReturn(Arrays.asList(event1))
                .thenReturn(Collections.emptyList());
        
        when(storage.get(new StatusEventID("foo1"))).thenReturn(Optional.absent());
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(2));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
        
        verify(logger).logError("Event foo1 is in the in-memory queue but not " +
                            "in the storage system. Removing from queue");
    }
    
    @Test
    public void fatalErrorOnPull() throws Exception {
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor,
                Arrays.asList(1, 1));
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenThrow(
                new FatalRetriableIndexingException("wheee!"));
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));
        
        verify(executor).shutdown();

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
                new FatalRetriableIndexingException("wheee!"),
                new FatalRetriableIndexingException("wheee!"),
                new FatalIndexingException("wheee!"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    @Test
    public void unexpectedErrorOnGetUnprocessed() throws Exception {
        
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor,
                Arrays.asList(1, 1));
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenThrow(
                new RuntimeException("arg"));
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(0));

        verify(executor, never()).shutdown();
        
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
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor,
                Arrays.asList(1));
        
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.PUBLISH_ACCESS_GROUP)
                .withNullableAccessGroupID(2)
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenReturn(Arrays.asList(event1));
        
        when(storage.setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY)).thenThrow(
                        new FatalRetriableIndexingException("oof ouch owie my bones"));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(executor).shutdown();

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
                new FatalRetriableIndexingException("oof ouch owie my bones"),
                new FatalIndexingException("oof ouch owie my bones"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
    
    
    @Test
    public void fatalErrorOnGet() throws Exception {
        final StatusEventStorage storage = mock(StatusEventStorage.class);
        final LineLogger logger = mock(LineLogger.class);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, 3, executor,
                Arrays.asList(1, 1, 1));
        
        final StoredStatusEvent event1 = new StoredStatusEvent(StatusEvent.getBuilder(
                "WS", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS)
                .withNullableAccessGroupID(2)
                .withNullableObjectID("1")
                .build(),
                new StatusEventID("foo1"), StatusEventProcessingState.UNPROC, null, null);
        
        final Runnable coordRunner = getIndexerRunnable(executor, coord);
        
        when(storage.get(StatusEventProcessingState.UNPROC, 3)).thenReturn(Arrays.asList(event1));
        
        when(storage.get(new StatusEventID("foo1"))).thenThrow(
                        new FatalRetriableIndexingException("yay"));
        
        coordRunner.run();
        assertThat("incorrect cycle count", coord.getContinuousCycles(), is(0));
        assertThat("incorrect queue size", coord.getQueueSize(), is(1));
        
        verify(storage).setProcessingState(new StatusEventID("foo1"),
                StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY);
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
                new FatalRetriableIndexingException("yay"),
                new FatalRetriableIndexingException("yay"),
                new FatalRetriableIndexingException("yay"),
                new FatalIndexingException("yay"));
        
        for (int i = 0; i < expected.size(); i++) {
            TestCommon.assertExceptionCorrect(retExpCaptor.getAllValues().get(i), expected.get(i));
        }
    }
}
