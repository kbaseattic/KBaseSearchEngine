package kbasesearchengine.main;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import kbasesearchengine.events.EventQueue;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.Retrier;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.tools.Utils;

/** Coordinates which events will get processed by {@link IndexerWorker}s based on the
 * {@link EventQueue}. The responsibility of the coordinator is to periodically update the event
 * state in the {@link StatusEventStorage} such that the workers process the correct events.
 * 
 * Only one indexer coordinator should run at one time.
 * 
 * This class is not thread safe.
 * @author gaprice@lbl.gov
 *
 */
public class IndexerCoordinator {
    
    private static final int RETRY_COUNT = 5;
    private static final int RETRY_SLEEP_MS = 1000;
    private static final List<Integer> RETRY_FATAL_BACKOFF_MS = Arrays.asList(
            1000, 2000, 4000, 8000, 16000);
    
    private final StatusEventStorage storage;
    private final LineLogger logger;
    private final ScheduledExecutorService executor;
    private final EventQueue queue;
    
    private final int maxQueueSize;
    private int continuousCycles = 0;
    
    private final Retrier retrier = new Retrier(RETRY_COUNT, RETRY_SLEEP_MS,
            RETRY_FATAL_BACKOFF_MS,
            (retrycount, event, except) -> logError(retrycount, event, except));

    /** Create the indexer coordinator. Only one coordinator should run at one time.
     * @param storage the storage system containing events.
     * @param logger a logger.
     * @param maximumQueueSize the maximum number of events in the internal in-memory queue.
     * This should be a fairly large number because events may not arrive in the storage system
     * in the ordering of their timestamps, and so the queue acts as a buffer so events can be
     * sorted before processing if an event arrives late.
     * @throws InterruptedException if the thread is interrupted while attempting to initialize
     * the coordinator.
     * @throws IndexingException if an exception occurs while trying to initialize the
     * coordinator.
     */
    public IndexerCoordinator(
            final StatusEventStorage storage,
            final LineLogger logger,
            final int maximumQueueSize)
            throws InterruptedException, IndexingException {
        this(storage, logger, maximumQueueSize, Executors.newSingleThreadScheduledExecutor());
    }
    
    /** Create an indexer coordinator solely for the purposes of testing. This constructor should
     * not be used for any other purpose.
     * @param storage the storage system containing events.
     * @param logger a logger.
     * @param maximumQueueSize the maximum number of events in the internal in-memory queue.
     * @param testExecutor a single thread executor for testing purposes, usually a mock.
     * @throws InterruptedException if the thread is interrupted while attempting to initialize
     * the coordinator.
     * @throws IndexingException if an exception occurs while trying to initialize the
     * coordinator.
     */
    public IndexerCoordinator(
            final StatusEventStorage storage,
            final LineLogger logger,
            final int maximumQueueSize,
            final ScheduledExecutorService testExecutor)
            throws InterruptedException, IndexingException {
        Utils.nonNull(storage, "storage");
        Utils.nonNull(logger, "logger");
        if (maximumQueueSize < 1) {
            throw new IllegalArgumentException("maximumQueueSize must be at least 1");
        }
        this.maxQueueSize = maximumQueueSize;
        this.logger = logger;
        this.storage = storage;
        final List<StoredStatusEvent> all = new LinkedList<>();
        all.addAll(retrier.retryFunc(
                s -> s.get(StatusEventProcessingState.READY, maxQueueSize), storage, null));
        all.addAll(retrier.retryFunc(
                s -> s.get(StatusEventProcessingState.PROC, maxQueueSize), storage, null));
        queue = new EventQueue(all);
        executor = testExecutor;
    }
    
    /** Get the maximum size of the in memory queue.
     * @return
     */
    public int getMaximumQueueSize() {
        return maxQueueSize;
    }
    
    /** Start the indexer. */
    public void startIndexer() {
        // may want to make this configurable
        executor.scheduleAtFixedRate(new IndexerRunner(), 0, 1000, TimeUnit.MILLISECONDS);
    }
    
    private class IndexerRunner implements Runnable {

        @Override
        public void run() {
            try {
                runOneCycle();
            } catch (InterruptedException | FatalIndexingException e) {
                logError(ErrorType.FATAL, e);
                executor.shutdown();
            } catch (Exception e) {
                logError(ErrorType.UNEXPECTED, e);
            }
        }
    }
    
    /** Stop the indexer. The current indexer cycle will complete and the indexer will then
     * process no more events.
     */
    public void stopIndexer() {
        executor.shutdown();
    }
    
    private enum ErrorType {
        FATAL, UNEXPECTED;
    }
    
    private void logError(final ErrorType errtype, final Throwable e) {
        Utils.nonNull(errtype, "errtype");
        final String msg;
        if (ErrorType.FATAL.equals(errtype)) {
            msg = "Fatal error in indexer, shutting down: ";
        } else if (ErrorType.UNEXPECTED.equals(errtype)) {
            msg = "Unexpected error in indexer: ";
        } else {
            throw new RuntimeException("Unknown error type: " + errtype);
        }
        logError(msg, e);
    }

    private void logError(final String msg, final Throwable e) {
        final String firstStackLine = e.getStackTrace().length == 0 ? "<not-available>" : 
                e.getStackTrace()[0].toString();
        logger.logError(msg + e + ", " + firstStackLine);
        logger.logError(e); //TODO LOG split into lines with id
    }

    private void logError(
            final int retrycount,
            final Optional<StoredStatusEvent> event,
            final RetriableIndexingException e) {
        final String msg;
        if (event.isPresent()) {
            msg = String.format("Retriable error in indexer for event %s %s, retry %s: ",
                    event.get().getEvent().getEventType(), event.get().getId().getId(),
                    retrycount);
        } else {
            msg = String.format("Retriable error in indexer, retry %s: ", retrycount);
        }
        logError(msg, e);
    }
    
    private void runOneCycle() throws InterruptedException, IndexingException {
        // some of these ops could be batched if they prove to be a bottleneck
        // but the mongo client keeps a connection open so it's not that expensive to 
        // run one at a time
        // also the bottleneck is almost assuredly the workers
        continuousCycles = 0;
        //TODO QUEUE check for stalled events
        boolean noWait = true;
        while (noWait) {
            final List<StoredStatusEvent> events;
            final int loadSize = maxQueueSize - queue.size();
            if (loadSize > 0) {
                events = retrier.retryFunc(s -> s.get(StatusEventProcessingState.UNPROC, loadSize),
                        storage, null);
                events.stream().forEach(e -> queue.load(e));
            } else {
                events = Collections.emptyList();
            }
            queue.moveToReady();
            for (final StoredStatusEvent sse: queue.getReadyForProcessing()) {
                // since the queue doesn't mutate the state, if the state is not UNPROC
                // it's not in that state in the DB either
                if (sse.getState().equals(StatusEventProcessingState.UNPROC)) {
                    //TODO QUEUE mark with timestamp
                    retrier.retryCons(e -> storage.setProcessingState(e.getId(),
                            StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY),
                            sse, sse);
                    //TODO QUEUE LOG this
                }
            }
            // so we don't run through the same events again next loop
            queue.moveReadyToProcessing();
            for (final StoredStatusEvent sse: queue.getProcessing()) {
                final Optional<StoredStatusEvent> fromStorage =
                        retrier.retryFunc(s -> s.get(sse.getId()), storage, sse);
                if (fromStorage.isPresent()) {
                    final StatusEventProcessingState state = fromStorage.get().getState();
                    if (!state.equals(StatusEventProcessingState.PROC) &&
                            !state.equals(StatusEventProcessingState.READY)) {
                        queue.setProcessingComplete(fromStorage.get());
                        //TODO QUEUE LOG this
                    } else {
                        //TODO QUEUE check time since last update and log if > X min (log periodically, maybe 1 /hr)
                    }
                } else {
                    logger.logError(String.format("Event %s is in the in-memory queue but not " +
                            "in the storage system. Removing from queue", sse.getId()));
                    queue.setProcessingComplete(sse);
                }
            }
            noWait = !events.isEmpty() && queue.size() < maxQueueSize;
            continuousCycles++;
        }
    }
    
    /** Returns the number of cycles the indexer has run without pausing (e.g. without the
     * indexer cycle being scheduled). This information is mostly useful for test purposes.
     * @return 
     */
    public int getContinuousCycles() {
        return continuousCycles;
    }
    
    /** Returns the current size of the queue.
     * @return the queue size.
     */
    public int getQueueSize() {
        return queue.size();
    }
}
