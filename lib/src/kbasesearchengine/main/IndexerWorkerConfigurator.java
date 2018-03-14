package kbasesearchengine.main;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.tools.Utils;

/** A configurator for an {@link IndexerWorker}.
 * @author gaprice@lbl.gov
 *
 */
public class IndexerWorkerConfigurator {
    
    private final String id;
    private final Path rootTempDir;
    private final StatusEventStorage eventStorage;
    private final TypeStorage typeStorage;
    private final IndexingStorage indexingStorage;
    private final Set<String> workerCodes;
    private final LineLogger logger;
    private final Map<String, EventHandler> eventHandlers;
    private final int maxObjectsPerLoad;
    private final int retryCount;
    private final int retrySleepMS;
    private final List<Integer> retryFatalBackoffMS;
    
    private IndexerWorkerConfigurator(
            final String id,
            final Path rootTempDir,
            final StatusEventStorage eventStorage,
            final TypeStorage typeStorage,
            final IndexingStorage indexingStorage,
            final Set<String> workerCodes,
            final LineLogger logger,
            final Map<String, EventHandler> eventHandlers,
            final int maxObjectsPerLoad,
            final int retryCount,
            final int retrySleepMS,
            final List<Integer> retryFatalBackoffMS) {
        this.id = id;
        this.rootTempDir = rootTempDir;
        this.eventStorage = eventStorage;
        this.typeStorage = typeStorage;
        this.indexingStorage = indexingStorage;
        this.workerCodes = Collections.unmodifiableSet(workerCodes);
        this.logger = logger;
        this.eventHandlers = Collections.unmodifiableMap(eventHandlers);
        this.maxObjectsPerLoad = maxObjectsPerLoad;
        this.retryCount = retryCount;
        this.retrySleepMS = retrySleepMS;
        this.retryFatalBackoffMS = Collections.unmodifiableList(retryFatalBackoffMS);
    }

    /** Get the ID of the worker.
     * @return the worker's ID.
     */
    public String getWorkerID() {
        return id;
    }

    /** Get the temporary directory the worker should use to store temporary files and
     * subdirectories.
     * @return the temporary directory.
     */
    public Path getRootTempDir() {
        return rootTempDir;
    }

    /** Get the event storage system.
     * @return the event storage system.
     */
    public StatusEventStorage getEventStorage() {
        return eventStorage;
    }

    /** Get the type storage system.
     * @return the type storage system.
     */
    public TypeStorage getTypeStorage() {
        return typeStorage;
    }

    /** Get the indexing storage system.
     * @return the indexing storage system.
     */
    public IndexingStorage getIndexingStorage() {
        return indexingStorage;
    }

    /** Get the worker codes for the worker. Worker codes determine which events the worker will
     * process. No codes implies the worker will only process events with the code 'default'.
     * @return the worker codes.
     */
    public Set<String> getWorkerCodes() {
        return workerCodes;
    }

    /** Get the logger for the worker.
     * @return the logger.
     */
    public LineLogger getLogger() {
        return logger;
    }

    /** Get the event handlers for the worker. Each handler is mapped from the storage code of
     * the data source to which the handler applies. For example, a workspace service handler's
     * {@link EventHandler#getStorageCode()} method would return "WS", and so the mapping would
     * be WS -> handler.
     * @return the event handlers.
     */
    public Map<String, EventHandler> getEventHandlers() {
        return eventHandlers;
    }

    /** Get the maximum number of objects that may be loaded into the indexing system at once.
     * If more objects are attempted, an error will occur.
     * @return the maximum number of objects to load in the indexing system.
     */
    public int getMaxObjectsPerLoad() {
        return maxObjectsPerLoad;
    }
    
    /** Get the number of times the worker should retry non-fatal functions before giving up and
     * marking an event as failed.
     * @return the number of times the worker should retry non-fatal functions.
     */
    public int getRetryCount() {
        return retryCount;
    }

    /** Get the sleep time in milliseconds between retries for non-fatal functions.
     * @return the sleep time.
     */
    public int getRetrySleepMS() {
        return retrySleepMS;
    }

    /** Get the backoff times in milliseconds between retries for fatal functions.
     * This is typically an exponential backoff. When the last retry fails, the worker shuts down.
     * @return the back off times.
     */
    public List<Integer> getRetryFatalBackoffMS() {
        return retryFatalBackoffMS;
    }

    /** Get a builder for a {@link IndexerWorkerConfigurator}. Note that a call to
     * {@link Builder#withStorage(StatusEventStorage, TypeStorage, IndexingStorage)} is required
     * before calling {@link Builder#build()}.
     * @param workerID the arbitrary ID of the worker to be configured. This ID must be unique
     * among all running workers.
     * @param rootTempDirectory a temporary directory for the worker to use for temporary files
     * and subdirectories.
     * @param logger a logger.
     * @return a new builder.
     */
    public static Builder getBuilder(
            final String workerID,
            final Path rootTempDirectory,
            final LineLogger logger) {
        return new Builder(workerID, rootTempDirectory, logger);
    }
    
    /** A builder for an {@link IndexerWorkerConfigurator}.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final String id;
        private final LineLogger logger;
        private final Path rootTempDir;
        private StatusEventStorage eventStorage = null;
        private TypeStorage typeStorage = null;
        private IndexingStorage indexingStorage = null;
        private final Map<String, EventHandler> eventHandlers = new HashMap<>();
        private final Set<String> workerCodes = new HashSet<>();
        private int maxObjectsPerLoad = 100_000;
        private int retryCount = 5;
        private int retrySleepMS = 1000;
        private List<Integer> retryFatalBackOffMS = Arrays.asList(1000, 2000, 4000, 8000, 16000);
        
        private Builder(
                final String workerID,
                final Path rootTempDirectory,
                final LineLogger logger) {
            Utils.notNullOrEmpty(workerID, "workerID cannot be null or whitespace only");
            Utils.nonNull(rootTempDirectory, "rootTempDirectory");
            Utils.nonNull(logger, "logger");
            this.id = workerID;
            this.logger = logger;
            this.rootTempDir = rootTempDirectory;
        }
        
        /** Add required storage systems to the worker configurator.
         * @param eventStorage the event storage system.
         * @param typeStorage the type storage system.
         * @param indexingStorage the indexing storage system.
         * @return this builder.
         */
        public Builder withStorage(
                final StatusEventStorage eventStorage,
                final TypeStorage typeStorage,
                final IndexingStorage indexingStorage) {
            Utils.nonNull(indexingStorage, "indexingStorage");
            Utils.nonNull(eventStorage, "eventStorage");
            Utils.nonNull(typeStorage, "typeStorage");
            this.eventStorage = eventStorage;
            this.typeStorage = typeStorage;
            this.indexingStorage = indexingStorage;
            return this;
        }
        
        /** Add an event handler to the worker configurator.
         * @param handler the event handler.
         * @return this builder.
         */
        public Builder withEventHandler(final EventHandler handler) {
            Utils.nonNull(handler, "handler");
            if (eventHandlers.containsKey(handler.getStorageCode())) {
                throw new IllegalArgumentException(
                        "Already registered a handler for storage code " +
                        handler.getStorageCode());
            }
            eventHandlers.put(handler.getStorageCode(), handler);
            return this;
        }
        
        /** Add a worker code to the worker configurator. Worker codes determine which events
         * the worker will process. No codes implies the worker will only process events with
         * the code 'default'.
         * @param workerCode the worker code.
         * @return this builder.
         */
        public Builder withWorkerCode(final String workerCode) {
            Utils.notNullOrEmpty(workerCode, "workerCode cannot be null or whitespace only");
            workerCodes.add(workerCode);
            return this;
        }
        
        /** Add the maximum number of objects that may be loaded into the indexing system at once
         * to the configurator. If more objects are attempted, an error will occur.
         * @param maxObjects the maximum objects to load into the indexing system at once.
         * @return this builder.
         */
        public Builder withMaxObjectsPerIndexingLoad(final int maxObjects) {
            if (maxObjects < 1) {
                throw new IllegalArgumentException("maxObjects must be at least 1");
            }
            this.maxObjectsPerLoad = maxObjects;
            return this;
        }
        
        /** Add the number of times the worker should retry non-fatal functions before giving up
         * and marking an event as failed to the configurator.
         * The default is 5 retries.
         * @param retryCount the retry count.
         * @return this builder.
         */
        public Builder withRetryCount(final int retryCount) {
            if (retryCount < 1) {
                throw new IllegalArgumentException("retryCount must be at least 1");
            }
            this.retryCount = retryCount;
            return this;
        }
        
        /** Add the sleep time in milliseconds between retries for non-fatal functions to the
         * configurator.
         * The default is 1 second.
         * @param retrySleepMS the sleep time.
         * @return this builder.
         */
        public Builder withRetrySleepTimeMS(final int retrySleepMS) {
            if (retrySleepMS < 1) {
                throw new IllegalArgumentException("retrySleepMS must be at least 1");
            }
            this.retrySleepMS = retrySleepMS;
            return this;
        }
        
        /** Add the backoff times in milliseconds between retries for fatal functions to the
         * configurator.
         * This is typically an exponential backoff. When the last retry fails, the worker shuts
         * down.
         * 
         * The default is 1, 2, 4, 8, and 16 seconds.
         * @param retryBackoffMS the backoff times.
         * @return this builder.
         */
        public Builder withRetryFatalBackoffTimeMS(final Integer... retryBackoffMS) {
            Utils.nonNull(retryBackoffMS, "retryBackoffMS");
            final List<Integer> retry = Arrays.asList(retryBackoffMS);
            if (retry.isEmpty()) {
                throw new IllegalArgumentException("Must provide at least one retry backoff time");
            }
            for (final Integer i : retry) {
                if (i == null || i < 1) {
                    throw new IllegalArgumentException(
                            "The entries of retryBackoffMS must be non-null and at least 1");
                }
            }
            this.retryFatalBackOffMS = retry;
            return this;
        }
        
        /** Build the {@link IndexerWorkerConfigurator}.
         * @return a new {@link IndexerWorkerConfigurator}.
         */
        public IndexerWorkerConfigurator build() {
            if (eventStorage == null) {
                throw new IllegalArgumentException("storage systems must be set");
            }
            return new IndexerWorkerConfigurator(id, rootTempDir, eventStorage, typeStorage,
                    indexingStorage, workerCodes, logger, eventHandlers, maxObjectsPerLoad,
                    retryCount, retrySleepMS, retryFatalBackOffMS);
        }
    }

}
