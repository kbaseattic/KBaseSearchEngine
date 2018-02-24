package kbasesearchengine.main;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.base.Optional;

import kbasesearchengine.common.FileUtil;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventWithId;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.IndexingExceptionUncheckedWrapper;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingExceptionUncheckedWrapper;
import kbasesearchengine.events.exceptions.Retrier;
import kbasesearchengine.events.exceptions.UnprocessableEventIndexingException;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.ResolvedReference;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.parse.KeywordParser.ObjectLookupProvider;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.system.NoSuchTypeException;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ParsingRulesSubtypeFirstComparator;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.tools.Utils;
import org.apache.commons.io.FileUtils;

public class IndexerWorker implements Stoppable {
    
    //TODO JAVADOC
    //TODO TESTS
    
    private static final int RETRY_COUNT = 5;
    private static final int RETRY_SLEEP_MS = 1000;
    private static final List<Integer> RETRY_FATAL_BACKOFF_MS = Arrays.asList(
            1000, 2000, 4000, 8000, 16000);

    private final String id;
    private final File rootTempDir;
    private final StatusEventStorage storage;
    private final TypeStorage typeStorage;
    private final IndexingStorage indexingStorage;
    private final Set<String> workerCodes;
    private final LineLogger logger;
    private final Map<String, EventHandler> eventHandlers = new HashMap<>();
    private ScheduledExecutorService executor = null;
    private final SignalMonitor signalMonitor = new SignalMonitor();
    private boolean stopRunner = false;
    private final int maxObjectsPerLoad;
    
    private final Retrier retrier = new Retrier(RETRY_COUNT, RETRY_SLEEP_MS,
            RETRY_FATAL_BACKOFF_MS,
            (retrycount, event, except) -> logError(retrycount, event, except));

    public IndexerWorker(
            final String id,
            final List<EventHandler> eventHandlers,
            final StatusEventStorage storage,
            final IndexingStorage indexingStorage,
            final TypeStorage typeStorage,
            final File tempDir,
            final LineLogger logger,
            final Set<String> workerCodes,
            final int maxObjectsPerLoad)
            throws IOException {
        Utils.notNullOrEmpty("id", "id cannot be null or the empty string");
        Utils.nonNull(logger, "logger");
        Utils.nonNull(indexingStorage, "indexingStorage");
        this.maxObjectsPerLoad = maxObjectsPerLoad;
        this.workerCodes = workerCodes;
        logger.logInfo("Worker codes: " + workerCodes);
        this.id = id;
        this.logger = logger;
        this.rootTempDir = FileUtil.getOrCreateCleanSubDir(tempDir,
                id + "_" + UUID.randomUUID().toString().substring(0,5));
        logger.logInfo("Created temp dir " + rootTempDir.getAbsolutePath() +
                                                     " for indexer worker " + id);
        
        eventHandlers.stream().forEach(eh -> this.eventHandlers.put(eh.getStorageCode(), eh));
        this.storage = storage;
        this.typeStorage = typeStorage;
        this.indexingStorage = indexingStorage;
    }
    
    @Override
    public void awaitShutdown() throws InterruptedException {
        signalMonitor.awaitSignal();
    }
    
    public void startIndexer() {
        stopRunner = false;
        //TODO TEST add a way to inject an executor for testing purposes
        executor = Executors.newSingleThreadScheduledExecutor();
        // may want to make this configurable
        executor.scheduleAtFixedRate(new IndexerRunner(), 0, 1000, TimeUnit.MILLISECONDS);
    }
    
    private class IndexerRunner implements Runnable {

        @Override
        public void run() {
            boolean processedEvent = true;
            while (!stopRunner && processedEvent) {
                processedEvent = false;
                try {
                    // keep processing events until there are none left
                    processedEvent = performOneTick();
                } catch (InterruptedException | FatalIndexingException e) {
                    logError(ErrorType.FATAL, e);
                    executor.shutdown();
                    signalMonitor.signal();
                } catch (Throwable e) {
                    logError(ErrorType.UNEXPECTED, e);
                }
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

        try {
            FileUtils.deleteDirectory(rootTempDir);
        } catch(IOException ioe) {
            logger.logError("Unable to delete worker temp dir "+rootTempDir+
                    " on shutdown of worker with id "+id+
                    ". Please delete manually. "+ioe.getMessage());
        }
    }
    
    private enum ErrorType {
        STD, FATAL, UNEXPECTED;
    }
    
    private void logError(final ErrorType errtype, final Throwable e) {
        Utils.nonNull(errtype, "errtype");
        final String msg;
        if (ErrorType.FATAL.equals(errtype)) {
            msg = "Fatal error in indexer, shutting down";
        } else if (ErrorType.STD.equals(errtype)) {
            msg = "Error in indexer";
        } else if (ErrorType.UNEXPECTED.equals(errtype)) {
            msg = "Unexpected error in indexer";
        } else {
            throw new RuntimeException("Unknown error type: " + errtype);
        }
        logError(msg, e);
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
            msg = String.format("Retriable error in indexer for event %s %s%s, retry %s",
                    event.get().getEvent().getEventType(),
                    event.get().isParentId() ? "with parent ID " : "",
                    event.get().getId().getId(),
                    retrycount);
        } else {
            msg = String.format("Retriable error in indexer, retry %s", retrycount);
        }
        logError(msg, e);
    }
    
    private boolean performOneTick() throws InterruptedException, IndexingException {
        final Optional<StoredStatusEvent> optEvent = retrier.retryFunc(
                s -> s.setAndGetProcessingState(StatusEventProcessingState.READY, workerCodes,
                        StatusEventProcessingState.PROC, id),
                storage, null);
        boolean processedEvent = false;
        if (optEvent.isPresent()) {
            final StoredStatusEvent parentEvent = optEvent.get();
            final EventHandler handler;
            try {
                handler = getEventHandler(parentEvent);
            } catch (UnprocessableEventIndexingException e) {
                logError(ErrorType.STD, e);
                markEventProcessed(parentEvent, StatusEventProcessingState.FAIL);
                return true;
            }
            if (handler.isExpandable(parentEvent)) {
                expandAndProcess(parentEvent);
            } else {
                markEventProcessed(parentEvent, processEvent(parentEvent));
            }
            processedEvent = true;
        }
        return processedEvent;
    }

    private void markEventProcessed(
            final StoredStatusEvent parentEvent,
            final StatusEventProcessingState result)
            throws InterruptedException, FatalIndexingException {
        try {
            // should only throw fatal
            retrier.retryCons(s -> s.setProcessingState(parentEvent.getId(),
                    StatusEventProcessingState.PROC, result), storage, parentEvent);
        } catch (FatalIndexingException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            // don't know how to respond to anything else, so mark event failed and keep going
            logError(ErrorType.UNEXPECTED, e);
            markAsVisitedFailedPostError(parentEvent);
        }
    }

    private void expandAndProcess(final StoredStatusEvent parentEvent)
            throws FatalIndexingException, InterruptedException {
        logger.logInfo(String.format("[Indexer] Expanding event %s %s",
                parentEvent.getEvent().getEventType(), parentEvent.getId().getId()));
        final Iterator<ChildStatusEvent> childIter;
        try {
            childIter = retrier.retryFunc(e -> getSubEventIterator(e), parentEvent, parentEvent);
        } catch (IndexingException e) {
            markAsVisitedFailedPostError(parentEvent);
            handleException("Error expanding parent event", parentEvent, e);
            return;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            // don't know how to respond to anything else, so mark event failed and keep going
            logError(ErrorType.UNEXPECTED, e);
            markAsVisitedFailedPostError(parentEvent);
            return;
        }
        StatusEventProcessingState parentResult = StatusEventProcessingState.INDX;
        while (childIter.hasNext()) {
            ChildStatusEvent subev = null;
            try {
                subev = retrier.retryFunc(i -> getNextSubEvent(i), childIter, parentEvent);
            } catch (IndexingException e) {
                handleException("Error getting event information from data storage",
                        parentEvent, e);
                parentResult = StatusEventProcessingState.FAIL;
            }
            if (subev != null && StatusEventProcessingState.FAIL.equals(processEvent(subev))) {
                parentResult = StatusEventProcessingState.FAIL;
            }
        }
        markEventProcessed(parentEvent, parentResult);
    }
    
    private Iterator<ChildStatusEvent> getSubEventIterator(final StoredStatusEvent ev)
            throws IndexingException, RetriableIndexingException {
        try {
            return getEventHandler(ev).expand(ev).iterator();
        } catch (IndexingExceptionUncheckedWrapper e) {
            throw e.getIndexingException();
        } catch (RetriableIndexingExceptionUncheckedWrapper e) {
            throw e.getIndexingException();
        }
    }

    // assumes something has already failed, so if this fails as well something is really 
    // wrong and we bail.
    private void markAsVisitedFailedPostError(final StoredStatusEvent parentEvent)
            throws FatalIndexingException {
        try {
            storage.setProcessingState(parentEvent.getId(), null, StatusEventProcessingState.FAIL);
        } catch (Exception e) {
            //ok then we're screwed
            throw new FatalIndexingException("Can't mark events as failed: " + e.getMessage(), e);
        }
    }

    private StatusEventProcessingState processEvent(final StatusEventWithId ev)
            throws InterruptedException, FatalIndexingException {
        final Optional<StorageObjectType> type = ev.getEvent().getStorageObjectType();
        if (type.isPresent() && !isStorageTypeSupported(ev)) {
            logger.logInfo("[Indexer] skipping " + ev.getEvent().getEventType() + ", " + 
                    toLogString(type) + ev.getEvent().toGUID());
            return StatusEventProcessingState.UNINDX;
        }
        logger.logInfo("[Indexer] processing " + ev.getEvent().getEventType() + ", " + 
                toLogString(type) + ev.getEvent().toGUID() + "...");
        final long time = System.currentTimeMillis();
        try {
            retrier.retryCons(e -> processOneEvent(e), ev.getEvent(), ev);
        } catch (IndexingException e) {
            handleException("Error processing event", ev, e);
            return StatusEventProcessingState.FAIL;
        }
        logger.logInfo("[Indexer]   (total time: " + (System.currentTimeMillis() - time) + "ms.)");
        return StatusEventProcessingState.INDX;
    }
    
    private ChildStatusEvent getNextSubEvent(Iterator<ChildStatusEvent> iter)
            throws IndexingException, RetriableIndexingException {
        try {
            return iter.next();
        } catch (IndexingExceptionUncheckedWrapper e) {
            throw e.getIndexingException();
        } catch (RetriableIndexingExceptionUncheckedWrapper e) {
            throw e.getIndexingException();
        }
    }
    
    private void handleException(
            final String error,
            final StatusEventWithId event,
            final IndexingException exception)
            throws FatalIndexingException {
        if (exception instanceof FatalIndexingException) {
            throw (FatalIndexingException) exception;
        } else {
            final String msg = error + String.format(" for event %s %s%s",
                    event.getEvent().getEventType(),
                    event.isParentId() ? "with parent ID " : "",
                    event.getId().getId());
            logError(msg, exception);
        }
    }

    private String toLogString(final Optional<StorageObjectType> type) {
        if (!type.isPresent()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(type.get().getStorageCode());
        sb.append(":");
        sb.append(type.get().getType());
        if (type.get().getVersion().isPresent()) {
            sb.append("-");
            sb.append(type.get().getVersion().get());
        }
        sb.append(", ");
        return sb.toString();
    }

    private EventHandler getEventHandler(final StoredStatusEvent ev)
            throws UnprocessableEventIndexingException {
        return getEventHandler(ev.getEvent().getStorageCode());
    }
    
    private EventHandler getEventHandler(final GUID guid)
            throws UnprocessableEventIndexingException {
        return getEventHandler(guid.getStorageCode());
    }
    
    private EventHandler getEventHandler(final String storageCode)
            throws UnprocessableEventIndexingException {
        if (!eventHandlers.containsKey(storageCode)) {
            throw new UnprocessableEventIndexingException(String.format(
                    "No event handler for storage code %s is registered", storageCode));
        }
        return eventHandlers.get(storageCode);
    }

    public void processOneEvent(final StatusEvent ev)
            throws IndexingException, InterruptedException, RetriableIndexingException {
        try {
            switch (ev.getEventType()) {
            case NEW_VERSION:
                GUID pguid = ev.toGUID();
                boolean indexed = indexingStorage.checkParentGuidsExist(new LinkedHashSet<>(
                        Arrays.asList(pguid))).get(pguid);
                if (indexed) {
                    logger.logInfo("[Indexer]   skipping " + pguid +
                            " creation (already indexed)");
                    // TODO: we should fix public access for all sub-objects too (maybe already works. Anyway, ensure all subobjects are set correctly as well as the parent)
                    if (ev.isPublic().get()) {
                        publish(pguid);
                    } else {
                        unpublish(pguid);
                    }
                } else {
                    indexObject(pguid, ev.getStorageObjectType().get(), ev.getTimestamp(),
                            ev.isPublic().get(), null, new LinkedList<>());
                }
                break;
            // currently unused
//            case DELETED:
//                unshare(ev.toGUID(), ev.getAccessGroupId().get());
//                break;
            case DELETE_ALL_VERSIONS:
                deleteAllVersions(ev.toGUID());
                break;
            case UNDELETE_ALL_VERSIONS:
                undeleteAllVersions(ev.toGUID());
                break;
                //TODO DP reenable if we support DPs
//            case SHARED:
//                share(ev.toGUID(), ev.getTargetAccessGroupId());
//                break;
                //TODO DP reenable if we support DPs
//            case UNSHARED:
//                unshare(ev.toGUID(), ev.getTargetAccessGroupId());
//                break;
            case RENAME_ALL_VERSIONS:
                renameAllVersions(ev.toGUID(), ev.getNewName().get());
                break;
            case PUBLISH_ALL_VERSIONS:
                publishAllVersions(ev.toGUID());
                break;
            case UNPUBLISH_ALL_VERSIONS:
                unpublishAllVersions(ev.toGUID());
                break;
            default:
                throw new UnprocessableEventIndexingException(
                        "Unsupported event type: " + ev.getEventType());
            }
        } catch (IOException e) {
            // may want to make IndexingStorage throw more specific exceptions, but this will work
            // for now. Need to look more carefully at the code before that happens.
            throw new RetriableIndexingException(e.getMessage(), e);
        }
    }

    // returns false if a non-fatal error prevents retrieving the info
    private boolean isStorageTypeSupported(final StatusEventWithId ev)
            throws InterruptedException, FatalIndexingException {
        try {
            return retrier.retryFunc(
                    t -> !typeStorage.listObjectTypeParsingRules(
                            ev.getEvent().getStorageObjectType().get()).isEmpty(),
                    ev, ev);
        } catch (IndexingException e) {
            handleException("Error retrieving type info", ev, e);
            return false;
        }
    }

    /** Index the object with the specified guid.
     *
     * @param guid an id that uniquely identifies the object that is to be indexed.
     * @param storageObjectType type of object that is to be indexed.
     * @param timestamp time at which this object was updated.
     * @param isPublic object access level (true if public, else false).
     * @param indexLookup
     * @param objectRefPath
     * @throws IndexingException
     * @throws InterruptedException
     * @throws RetriableIndexingException
     */
    private void indexObject(
            final GUID guid,
            final StorageObjectType storageObjectType,
            final Instant timestamp,
            final boolean isPublic,
            ObjectLookupProvider indexLookup,
            final List<GUID> objectRefPath) 
            throws IndexingException, InterruptedException, RetriableIndexingException {
        long t1 = System.currentTimeMillis();
        final File tempFile;
        try {
            FileUtil.getOrCreateSubDir(rootTempDir, guid.getStorageCode());
            tempFile = File.createTempFile("ws_srv_response_", ".json");
        } catch (IOException e) {
            throw new FatalRetriableIndexingException(e.getMessage(), e);
        }
        if (indexLookup == null) {
            indexLookup = new MOPLookupProvider();
        }
        try {
            // make a copy to avoid mutating the caller's path
            final LinkedList<GUID> newRefPath = new LinkedList<>(objectRefPath);
            newRefPath.add(guid);
            final EventHandler handler = getEventHandler(guid);
            final SourceData obj = handler.load(newRefPath, tempFile.toPath());
            long loadTime = System.currentTimeMillis() - t1;
            logger.logInfo("[Indexer]   " + guid + ", loading time: " + loadTime + " ms.");
            logger.timeStat(guid, loadTime, 0, 0);
            final List<ObjectTypeParsingRules> parsingRules = new ArrayList<>( 
                    typeStorage.listObjectTypeParsingRules(storageObjectType));
            Collections.sort(parsingRules, new ParsingRulesSubtypeFirstComparator());
            for (final ObjectTypeParsingRules rule : parsingRules) {
                final long t2 = System.currentTimeMillis();
                final ParseObjectsRet parsedRet = parseObjects(guid, indexLookup,
                        newRefPath, obj, rule);
                long parsingTime = System.currentTimeMillis() - t2;
                logger.logInfo("[Indexer]   " + toVerRep(rule.getGlobalObjectType()) +
                        ", parsing time: " + parsingTime + " ms.");
                long t3 = System.currentTimeMillis();
                indexObjectInStorage(guid, timestamp, isPublic, obj, rule,
                        parsedRet.guidToObj, parsedRet.parentJson);
                long indexTime = System.currentTimeMillis() - t3;
                logger.logInfo("[Indexer]   " + toVerRep(rule.getGlobalObjectType()) +
                        ", indexing time: " + indexTime + " ms.");
                logger.timeStat(guid, 0, parsingTime, indexTime);
            }
        } finally {
            tempFile.delete();
        }
    }

    private String toVerRep(final SearchObjectType globalObjectType) {
        return globalObjectType.getType() + "_" + globalObjectType.getVersion();
    }

    private void indexObjectInStorage(
            final GUID guid,
            final Instant timestamp,
            final boolean isPublic,
            final SourceData obj,
            final ObjectTypeParsingRules rule,
            final Map<GUID, ParsedObject> guidToObj,
            final String parentJson)
            throws InterruptedException, IndexingException {
        final List<?> input = Arrays.asList(rule, obj, timestamp, parentJson, guid, guidToObj,
                isPublic);
        retrier.retryCons(i -> indexObjectInStorage(i), input, null);
    }

    private void indexObjectInStorage(final List<?> input) throws FatalRetriableIndexingException {
        final ObjectTypeParsingRules rule = (ObjectTypeParsingRules) input.get(0);
        final SourceData obj = (SourceData) input.get(1);
        final Instant timestamp = (Instant) input.get(2);
        final String parentJson = (String) input.get(3);
        final GUID guid = (GUID) input.get(4);
        @SuppressWarnings("unchecked")
        final Map<GUID, ParsedObject> guidToObj = (Map<GUID, ParsedObject>) input.get(5);
        final Boolean isPublic = (Boolean) input.get(6);
        
        try {
            indexingStorage.indexObjects(
                    rule, obj, timestamp, parentJson, guid, guidToObj, isPublic);
        } catch (IOException e) {
            throw new FatalRetriableIndexingException(e.getMessage(), e);
        }
    }

    private class ParseObjectsRet {
        public final String parentJson;
        public final Map<GUID, ParsedObject> guidToObj;
        
        private ParseObjectsRet(final String parentJson, final Map<GUID, ParsedObject> guidToObj) {
            this.parentJson = parentJson;
            this.guidToObj = guidToObj;
        }
    }
    
    private ParseObjectsRet parseObjects(
            final GUID guid,
            final ObjectLookupProvider indexLookup,
            final LinkedList<GUID> newRefPath,
            final SourceData obj,
            final ObjectTypeParsingRules rule)
            throws IndexingException, InterruptedException {
        final List<?> inputs = Arrays.asList(guid, indexLookup, newRefPath, obj, rule);
        return retrier.retryFunc(i -> parseObjects(i), inputs, null);
    }
    
    private ParseObjectsRet parseObjects(final List<?> inputs)
            throws IndexingException, FatalRetriableIndexingException, InterruptedException {
        // should really wrap these in a class, but meh for now
        final GUID guid = (GUID) inputs.get(0);
        final ObjectLookupProvider indexLookup = (ObjectLookupProvider) inputs.get(1);
        @SuppressWarnings("unchecked")
        final List<GUID> newRefPath = (List<GUID>) inputs.get(2);
        final SourceData obj = (SourceData) inputs.get(3);
        final ObjectTypeParsingRules rule = (ObjectTypeParsingRules) inputs.get(4);

        final Map<GUID, ParsedObject> guidToObj = new HashMap<>();
        final String parentJson;
        try {
            try (JsonParser jts = obj.getData().getPlacedStream()) {
                parentJson = ObjectParser.extractParentFragment(rule, jts);
            }
            final Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(
                    obj, guid, rule);
            if (guidToJson.size() > maxObjectsPerLoad) {
                throw new UnprocessableEventIndexingException(String.format(
                        "Object %s has %s subobjects, exceeding the limit of %s",
                        guid, guidToJson.size(), maxObjectsPerLoad));
            }
            for (final GUID subGuid : guidToJson.keySet()) {
                final String json = guidToJson.get(subGuid);
                guidToObj.put(subGuid, KeywordParser.extractKeywords(
                        subGuid, rule.getGlobalObjectType(), json, parentJson,
                        rule.getIndexingRules(), indexLookup, newRefPath));
            }
            /* any errors here are due to file IO or parse exceptions.
             * Parse exceptions are def not retriable
             * File IO problems are generally going to mean something is very wrong
             * (like bad disk), since the file should already exist at this point.
             */
        } catch (ObjectParseException e) {
            throw new UnprocessableEventIndexingException(e.getMessage(), e);
        } catch (IOException e) {
            throw new FatalRetriableIndexingException(e.getMessage(), e);
        }
        return new ParseObjectsRet(parentJson, guidToObj);
    }
    
//    private void share(GUID guid, int accessGroupId) throws IOException {
//        indexingStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId, 
//                false);
//    }
    
    private void undeleteAllVersions(final GUID guid) throws IOException {
        indexingStorage.undeleteAllVersions(guid);
    }

//    private void unshare(GUID guid, int accessGroupId) throws IOException {
//        indexingStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
//    }

    private void deleteAllVersions(final GUID guid) throws IOException {
        indexingStorage.deleteAllVersions(guid);
    }

    private void publish(GUID guid) throws IOException {
        indexingStorage.publishObjects(new LinkedHashSet<>(Arrays.asList(guid)));
    }
    
    private void publishAllVersions(final GUID guid) throws IOException {
        indexingStorage.publishAllVersions(guid);
        //TODO DP need to handle objects in datapalette
    }

    private void unpublish(GUID guid) throws IOException {
        indexingStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(guid)));
    }
    
    private void unpublishAllVersions(final GUID guid) throws IOException {
        indexingStorage.unpublishAllVersions(guid);
        //TODO DP need to handle objects in datapalette
    }
    
    private void renameAllVersions(final GUID guid, final String newName) throws IOException {
        indexingStorage.setNameOnAllObjectVersions(guid, newName);
    }

    /** A lookup provider
     *
     */
    private class MOPLookupProvider implements ObjectLookupProvider {
        // storage code -> full ref path -> resolved guid
        private Map<String, Map<String, GUID>> refResolvingCache = new LinkedHashMap<>();
        private Map<GUID, ObjectData> objLookupCache = new LinkedHashMap<>();
        private Map<GUID, SearchObjectType> guidToTypeCache = new LinkedHashMap<>();
        
        @Override
        public Set<GUID> resolveRefs(List<GUID> callerRefPath, Set<GUID> refs)
                throws IndexingException, InterruptedException {
            /* the caller ref path 1) ensures that the object refs are valid when checked against
             * the source, and 2) allows getting deleted objects with incoming references 
             * in the case of the workspace
             */
            
            // there may be a way to cache more of this info and call the workspace less
            // by checking the ref against the refs in the parent object.
            // doing it the dumb way for now.
            final EventHandler eh = getEventHandler(callerRefPath.get(0));
            final String storageCode = eh.getStorageCode();
            if (!refResolvingCache.containsKey(storageCode)) {
                refResolvingCache.put(storageCode, new HashMap<>());
            }
            final Map<GUID, String> refToRefPath = eh.buildReferencePaths(callerRefPath, refs);
            Set<GUID> ret = new LinkedHashSet<>();
            Set<GUID> refsToResolve = new LinkedHashSet<>();
            for (final GUID ref : refs) {
                final String refpath = refToRefPath.get(ref);
                if (refResolvingCache.get(storageCode).containsKey(refpath)) {
                    ret.add(refResolvingCache.get(storageCode).get(refpath));
                } else {
                    refsToResolve.add(ref);
                }
            }
            if (refsToResolve.size() > 0) {
                final Set<ResolvedReference> resrefs =
                        resolveReferences(eh, callerRefPath, refsToResolve);
                for (final ResolvedReference rr: resrefs) {
                    final GUID guid = rr.getResolvedReference();
                    final boolean indexed = retrier.retryFunc(
                            g -> checkParentGuidExists(g), guid, null);
                    if (!indexed) {
                        indexObjectWrapperFn(guid, rr.getType(), rr.getTimestamp(), false,
                                this, callerRefPath);
                    }
                    ret.add(guid);
                    refResolvingCache.get(storageCode)
                            .put(refToRefPath.get(rr.getReference()), guid);
                }
            }
            return ret;
        }
        
        private boolean checkParentGuidExists(final GUID guid) throws RetriableIndexingException {
            try {
                return indexingStorage.checkParentGuidsExist(new HashSet<>(Arrays.asList(guid)))
                        .get(guid);
            } catch (IOException e) {
                throw new RetriableIndexingException(e.getMessage(), e);
            }
        }
        
        private Set<ResolvedReference> resolveReferences(
                final EventHandler eh,
                final List<GUID> callerRefPath,
                final Set<GUID> refsToResolve)
                throws IndexingException, InterruptedException {
            final List<Object> input = Arrays.asList(eh, callerRefPath, refsToResolve);
            return retrier.retryFunc(i -> resolveReferences(i), input, null);
        }
        
        private Set<ResolvedReference> resolveReferences(final List<Object> input)
                throws IndexingException, RetriableIndexingException {
            final EventHandler eh = (EventHandler) input.get(0);
            @SuppressWarnings("unchecked")
            final List<GUID> callerRefPath = (List<GUID>) input.get(1);
            @SuppressWarnings("unchecked")
            final Set<GUID> refsToResolve = (Set<GUID>) input.get(2);

            return eh.resolveReferences(callerRefPath, refsToResolve);
        }
        
        private void indexObjectWrapperFn(
                final GUID guid,
                final StorageObjectType storageObjectType,
                final Instant timestamp,
                final boolean isPublic,
                final ObjectLookupProvider indexLookup,
                final List<GUID> objectRefPath) 
                throws IndexingException, InterruptedException {
            final List<Object> input = Arrays.asList(guid, storageObjectType, timestamp, isPublic,
                    indexLookup, objectRefPath);
            retrier.retryCons(i -> indexObjectWrapperFn(i), input, null);
        }

        private void indexObjectWrapperFn(final List<Object> input)
                throws IndexingException, InterruptedException, RetriableIndexingException {
            final GUID guid = (GUID) input.get(0);
            final StorageObjectType storageObjectType = (StorageObjectType) input.get(1);
            final Instant timestamp = (Instant) input.get(2);
            final boolean isPublic = (boolean) input.get(3);
            final ObjectLookupProvider indexLookup = (ObjectLookupProvider) input.get(4);
            @SuppressWarnings("unchecked")
            final List<GUID> objectRefPath = (List<GUID>) input.get(5);
            
            indexObject(guid, storageObjectType, timestamp, isPublic, indexLookup, objectRefPath);
        }

        @Override
        public Map<GUID, ObjectData> lookupObjectsByGuid(final Set<GUID> guids)
                throws InterruptedException, IndexingException {
            Map<GUID, ObjectData> ret = new LinkedHashMap<>();
            Set<GUID> guidsToLoad = new LinkedHashSet<>();
            for (GUID guid : guids) {
                if (objLookupCache.containsKey(guid)) {
                    ret.put(guid, objLookupCache.get(guid));
                } else {
                    guidsToLoad.add(guid);
                }
            }
            if (guidsToLoad.size() > 0) {
                final List<ObjectData> objList =
                        retrier.retryFunc(g -> getObjectsByIds(g), guidsToLoad, null);
                // for some reason I don't understand a stream implementation would throw
                // duplicate key errors on the ObjectData, which is the value
                final Map<GUID, ObjectData> loaded = new HashMap<>();
                for (final ObjectData od: objList) {
                    loaded.put(od.getGUID(), od);
                }
                objLookupCache.putAll(loaded);
                ret.putAll(loaded);
            }
            return ret;
        }
        
        private List<ObjectData> getObjectsByIds(final Set<GUID> guids)
                throws RetriableIndexingException {
            kbasesearchengine.search.PostProcessing pp = 
                    new kbasesearchengine.search.PostProcessing();
            pp.objectData = false;
            pp.objectKeys = true;
            pp.objectInfo = true;
            try {
                return indexingStorage.getObjectsByIds(guids, pp);
            } catch (IOException e) {
                throw new RetriableIndexingException(e.getMessage(), e);
            }
        }
        
        @Override
        public ObjectTypeParsingRules getTypeDescriptor(final SearchObjectType type)
                throws IndexingException, NoSuchTypeException {
            return typeStorage.getObjectTypeParsingRules(type);
        }
        
        @Override
        public Map<GUID, SearchObjectType> getTypesForGuids(Set<GUID> guids)
                throws InterruptedException, IndexingException {
            Map<GUID, SearchObjectType> ret = new LinkedHashMap<>();
            Set<GUID> guidsToLoad = new LinkedHashSet<>();
            for (GUID guid : guids) {
                if (guidToTypeCache.containsKey(guid)) {
                    ret.put(guid, guidToTypeCache.get(guid));
                } else {
                    guidsToLoad.add(guid);
                }
            }
            if (guidsToLoad.size() > 0) {
                final List<ObjectData> data =
                        retrier.retryFunc(g -> getObjectsByIds(g), guidsToLoad, null);
                // for some reason I don't understand a stream implementation would throw
                // duplicate key errors on the od.getType(), which is the value
                final Map<GUID, SearchObjectType> loaded = new HashMap<>();
                for (final ObjectData od: data) {
                    loaded.put(od.getGUID(), od.getType().get());
                }
                guidToTypeCache.putAll(loaded);
                ret.putAll(loaded);
            }
            return ret;
        }
    }
}
