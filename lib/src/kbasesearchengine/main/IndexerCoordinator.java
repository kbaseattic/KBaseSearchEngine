package kbasesearchengine.main;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.Retrier;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.tools.Utils;

public class IndexerCoordinator {
    
    private static final int RETRY_COUNT = 5;
    private static final int RETRY_SLEEP_MS = 1000;
    private static final List<Integer> RETRY_FATAL_BACKOFF_MS = Arrays.asList(
            1000, 2000, 4000, 8000, 16000);
    
    private final StatusEventStorage storage;
    private final LineLogger logger;
    private ScheduledExecutorService executor = null;
    
    private final Retrier retrier = new Retrier(RETRY_COUNT, RETRY_SLEEP_MS,
            RETRY_FATAL_BACKOFF_MS,
            (retrycount, event, except) -> logError(retrycount, event, except));

    public IndexerCoordinator(
            final StatusEventStorage storage,
            final LineLogger logger) {
        Utils.nonNull(storage, "storage");
        Utils.nonNull(logger, "logger");
        this.logger = logger;
        this.storage = storage;
    }
    
    public void startIndexer() {
        executor = Executors.newSingleThreadScheduledExecutor();
        // may want to make this configurable
        executor.scheduleAtFixedRate(new IndexerRunner(), 0, 1000, TimeUnit.MILLISECONDS);
    }
    
    private class IndexerRunner implements Runnable {

        @Override
        public void run() {
            try {
                performOneTick();
            } catch (InterruptedException | FatalIndexingException e) {
                logError(ErrorType.FATAL, e);
                executor.shutdown();
            } catch (Exception e) {
                logError(ErrorType.UNEXPECTED, e);
            }
        }
    }
    
    public void stopIndexer() {
        executor.shutdown();
    }
    
    private enum ErrorType {
        STD, FATAL, UNEXPECTED;
    }
    
    private void logError(final ErrorType errtype, final Throwable e) {
        Utils.nonNull(errtype, "errtype");
        final String msg;
        if (ErrorType.FATAL.equals(errtype)) {
            msg = "Fatal error in indexer, shutting down: ";
        } else if (ErrorType.STD.equals(errtype)) {
            msg = "Error in indexer: ";
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
    
    private void performOneTick() throws InterruptedException, IndexingException {
        //TODO QUEUE intelligent queue
        //TODO QUEUE check for stalled events
        final List<StoredStatusEvent> parentEvents = retrier.retryFunc(
                s -> s.get(StatusEventProcessingState.UNPROC, 1000), storage, null);
        for (final StoredStatusEvent parentEvent: parentEvents) {
            retrier.retryCons(e -> storage.setProcessingState(e, StatusEventProcessingState.READY),
                    parentEvent, parentEvent);
        }
    }
}
