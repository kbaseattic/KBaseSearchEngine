package kbasesearchengine.main;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import kbasesearchengine.events.EventQueue;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventWithId;
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
public class IndexerCoordinator implements Stoppable {
    
    private static final int RETRY_COUNT = 5;
    private static final int RETRY_SLEEP_MS = 1000;
    private static final List<Integer> RETRY_FATAL_BACKOFF_MS_DEFAULT = Arrays.asList(
            1000, 2000, 4000, 8000, 16000);
    
    private final Cache<StatusEventID, Instant> cache;
    
    private final StatusEventStorage storage;
    private final LineLogger logger;
    private final ScheduledExecutorService executor;
    private final EventQueue queue;
    private final Clock clock;
    private final SignalMonitor signalMonitor;
    
    private final int maxQueueSize;
    private int continuousCycles = 0;
    private boolean stopRunner = false;
    
    private final Retrier retrier;

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
        this(
                storage,
                logger,
                new SignalMonitor(),
                maximumQueueSize,
                Executors.newSingleThreadScheduledExecutor(),
                RETRY_FATAL_BACKOFF_MS_DEFAULT,
                Ticker.systemTicker(), Clock.systemDefaultZone());
    }
    
    /** Create an indexer coordinator solely for the purposes of testing. This constructor should
     * not be used for any other purpose.
     * @param storage the storage system containing events.
     * @param logger a logger.
     * @param signalMonitor a monitor for detecting an internal shutdown.
     * @param maximumQueueSize the maximum number of events in the internal in-memory queue.
     * @param testExecutor a single thread executor for testing purposes, usually a mock.
     * @param retryFatalBackoffMS a list of times in milliseconds since the epoch. Starting with
     * the first item, any retriable commands, if failed, will wait for the specified number of
     * milliseconds prior to retrying the command. The number of retries is determined by the
     * number of items in the list.
     * @param ticker a time ticker that controls when records expire from the event id ->
     * last log cache. This cache controls how often log records are created for events that have
     * been processing or waiting for processing for a long time.
     * @param clock a clock for determining the current time.
     * @throws InterruptedException if the thread is interrupted while attempting to initialize
     * the coordinator.
     * @throws IndexingException if an exception occurs while trying to initialize the
     * coordinator.
     */
    public IndexerCoordinator(
            final StatusEventStorage storage,
            final LineLogger logger,
            final SignalMonitor signalMonitor,
            final int maximumQueueSize,
            final ScheduledExecutorService testExecutor,
            final List<Integer> retryFatalBackoffMS,
            final Ticker ticker,
            final Clock clock)
            throws InterruptedException, IndexingException {
        Utils.nonNull(storage, "storage");
        Utils.nonNull(logger, "logger");
        Utils.nonNull(signalMonitor, "signalMonitor");
        if (maximumQueueSize < 1) {
            throw new IllegalArgumentException("maximumQueueSize must be at least 1");
        }
        this.signalMonitor = signalMonitor;
        this.maxQueueSize = maximumQueueSize;
        this.logger = logger;
        this.storage = storage;
        final List<StoredStatusEvent> all = new LinkedList<>();
        retrier = new Retrier(RETRY_COUNT, RETRY_SLEEP_MS, retryFatalBackoffMS,
                (retrycount, event, except) -> logError(retrycount, event, except));
        all.addAll(retrier.retryFunc(
                s -> s.get(StatusEventProcessingState.READY, maxQueueSize), storage, null));
        all.addAll(retrier.retryFunc(
                s -> s.get(StatusEventProcessingState.PROC, maxQueueSize), storage, null));
        queue = new EventQueue(all);
        executor = testExecutor;
        this.clock = clock;
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    }
    
    @Override
    public void awaitShutdown() throws InterruptedException {
        signalMonitor.awaitSignal();
    }
    /** Get the maximum size of the in memory queue.
     * @return
     */
    public int getMaximumQueueSize() {
        return maxQueueSize;
    }
    
    /** Start the indexer. */
    public void startIndexer() {
        stopRunner = false;
        // may want to make this configurable
        executor.scheduleAtFixedRate(new IndexerRunner(), 0, 1000, TimeUnit.MILLISECONDS);
    }
    
    private class IndexerRunner implements Runnable {

        @Override
        public void run() {
            try {
                runOneCycle();
            } catch (InterruptedException | FatalIndexingException e) {
                logError(true, e);
                executor.shutdown();
                signalMonitor.signal();
            } catch (Throwable e) {
                logError(false, e);
            }
        }
    }
    
    @Override
    public void stop(long millisToWait) throws InterruptedException {
        if (millisToWait < 0) {
            millisToWait = 0;
        }
        stopRunner = true;
        executor.shutdown();
        executor.awaitTermination(millisToWait, TimeUnit.MILLISECONDS);
    }
    
    private void logError(final boolean fatal, final Throwable e) {
        logError(fatal ?
                "Fatal error in indexer, shutting down" : "Unexpected error in indexer", e);
    }

    private void logError(final String msg, final Throwable e) {
        // TODO LOG make log method that takes msg + e and have the logger figure out how to log it correctly
        logger.logError(msg + ": " + e);
        logger.logError(e);
    }

    private void logError(
            final int retrycount,
            final Optional<StatusEventWithId> event,
            final RetriableIndexingException e) {
        final String msg;
        if (event.isPresent()) {
            // no child events here, all ids are for the event itself
            msg = String.format("Retriable error in indexer for event %s %s, retry %s",
                    event.get().getEvent().getEventType(), event.get().getID().getId(),
                    retrycount);
        } else {
            msg = String.format("Retriable error in indexer, retry %s", retrycount);
        }
        logError(msg, e);
    }
    
    private void runOneCycle() throws InterruptedException, IndexingException {
        /* some of the operations in the submethods could be batched if they prove to be a
         * bottleneck
         * but the mongo client keeps a connection open so it's not that expensive to 
         * run one at a time
         * also the bottleneck is almost assuredly the workers
         */
        continuousCycles = 0;
        boolean noWait = true;
        while (!stopRunner && noWait) {
            final boolean loadedEvents = loadEventsIntoQueue();
            queue.moveToReady();
            setEventsAsReadyInStorage();
            // so we don't run through the same events again next loop
            queue.moveReadyToProcessing();
            checkOnEventsInProcess();
            // start the cycle immediately if there were events in storage and the queue isn't full
            noWait = loadedEvents && queue.size() < maxQueueSize;
            
            /* 18/2/21: the next line is a Q&D fix to stop a fast loop.
             * https://github.com/kbase/KBaseSearchEngine/pull/189 stopped the queue from
             * filling with duplicate events, but it means that if there are any unprocessed
             * events, they'll be loaded every cycle, which means that this loop continuously runs.
             * 
             * This change means the loop only runs 1/sec, period. When the coordinator is
             * smarter about loading only events that aren't in memory (or maybe not even
             * keeping unprocessed events in memory or something more drastic) the fast loop
             * behavior can be restored.
             * 
             */
            noWait = false;
            continuousCycles++;
        }
    }
    

    private boolean loadEventsIntoQueue() throws InterruptedException, IndexingException {
        final boolean loaded;
        final int loadSize = maxQueueSize - queue.size();
        if (loadSize > 0) {
            final List<StoredStatusEvent> events = retrier.retryFunc(
                    s -> s.get(StatusEventProcessingState.UNPROC, loadSize), storage, null);
            events.stream().forEach(e -> queue.load(e));
            loaded = !events.isEmpty();
        } else {
            loaded = false;
        }
        return loaded;
    }

    private void setEventsAsReadyInStorage() throws InterruptedException, IndexingException {
        for (final StoredStatusEvent sse: queue.getReadyForProcessing()) {
            // since the queue doesn't mutate the state, if the state is not UNPROC
            // it's not in that state in the DB either
            if (sse.getState().equals(StatusEventProcessingState.UNPROC)) {
                retrier.retryCons(e -> storage.setProcessingState(e.getID(),
                        StatusEventProcessingState.UNPROC, StatusEventProcessingState.READY),
                        sse, sse);
                logger.logInfo(String.format("Moved event %s %s %s from %s to %s",
                        sse.getID().getId(), sse.getEvent().getEventType(),
                        sse.getEvent().toGUID(), StatusEventProcessingState.UNPROC,
                        StatusEventProcessingState.READY));
            }
        }
    }
    
    private void checkOnEventsInProcess() throws InterruptedException, IndexingException {
        for (final StoredStatusEvent sse: queue.getProcessing()) {
            final Optional<StoredStatusEvent> fromStorage =
                    retrier.retryFunc(s -> s.get(sse.getID()), storage, sse);
            if (fromStorage.isPresent()) {
                final StoredStatusEvent e = fromStorage.get();
                final StatusEventProcessingState state = e.getState();
                if (!state.equals(StatusEventProcessingState.PROC) &&
                        !state.equals(StatusEventProcessingState.READY)) {
                    queue.setProcessingComplete(e);
                    logger.logInfo(String.format(
                            "Event %s %s %s completed processing with state %s on worker %s",
                            e.getID().getId(), e.getEvent().getEventType(),
                            e.getEvent().toGUID(), state, e.getUpdater().orNull()));
                } else {
                    logDelayedEvent(e);
                }
            } else {
                logger.logError(String.format("Event %s is in the in-memory queue but not " +
                        "in the storage system. Removing from queue", sse.getID().getId()));
                queue.setProcessingComplete(sse);
            }
        }
    }

    private void logDelayedEvent(final StoredStatusEvent e) {
        // this method should only be called on events that are READY or PROC and so will
        // always have an updateTime().
        final Instant lastStateChange = e.getUpdateTime().get();
        final Instant now = clock.instant();
        if (now.isAfter(lastStateChange.plus(1, ChronoUnit.HOURS))) {
            final Instant lastlog = cache.getIfPresent(e.getID());
            if (lastlog == null || now.isAfter(lastlog.plus(1, ChronoUnit.HOURS))) {
                cache.put(e.getID(), now);
                final long hours = Duration.between(lastStateChange, now).toHours();
                logger.logInfo(String.format(
                            "Event %s %s %s in state %s has been processing for %s hours " + 
                            "on worker %s",
                            e.getID().getId(), e.getEvent().getEventType(),
                            e.getEvent().toGUID(), e.getState(), hours, e.getUpdater().orNull()));
            }
        }
    }

    /** Returns the number of cycles the indexer has run without pausing (e.g. without the
     * indexer cycle being scheduled). This information is mostly useful for test purposes.
     * @return the number of cycles the indexer has run without pausing.
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
