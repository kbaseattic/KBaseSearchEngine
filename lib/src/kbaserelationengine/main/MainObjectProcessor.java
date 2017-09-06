package kbaserelationengine.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.core.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import kbaserelationengine.AccessFilter;
import kbaserelationengine.GetObjectsInput;
import kbaserelationengine.GetObjectsOutput;
import kbaserelationengine.KeyDescription;
import kbaserelationengine.MatchFilter;
import kbaserelationengine.MatchValue;
import kbaserelationengine.Pagination;
import kbaserelationengine.PostProcessing;
import kbaserelationengine.SearchObjectsInput;
import kbaserelationengine.SearchObjectsOutput;
import kbaserelationengine.SearchTypesInput;
import kbaserelationengine.SearchTypesOutput;
import kbaserelationengine.SortingRule;
import kbaserelationengine.TypeDescriptor;
import kbaserelationengine.common.GUID;
import kbaserelationengine.events.AccessGroupCache;
import kbaserelationengine.events.AccessGroupProvider;
import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.events.WorkspaceAccessGroupProvider;
import kbaserelationengine.events.handler.EventHandler;
import kbaserelationengine.events.handler.SourceData;
import kbaserelationengine.events.handler.WorkspaceEventHandler;
import kbaserelationengine.events.reconstructor.AccessType;
import kbaserelationengine.events.reconstructor.PresenceType;
import kbaserelationengine.events.reconstructor.Util;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructor;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbaserelationengine.events.storage.MongoDBStatusEventStorage;
import kbaserelationengine.events.storage.StatusEventStorage;
import kbaserelationengine.parse.KeywordParser;
import kbaserelationengine.parse.KeywordParser.ObjectLookupProvider;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.parse.ParsedObject;
import kbaserelationengine.queue.ObjectStatusEventIterator;
import kbaserelationengine.queue.ObjectStatusEventQueue;
import kbaserelationengine.relations.DefaultRelationStorage;
import kbaserelationengine.relations.RelationStorage;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.FoundHits;
import kbaserelationengine.search.IndexingStorage;
import kbaserelationengine.system.DefaultSystemStorage;
import kbaserelationengine.system.IndexingRules;
import kbaserelationengine.system.ObjectTypeParsingRules;
import kbaserelationengine.system.SystemStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.UObject;
import us.kbase.common.service.UnauthorizedException;
import workspace.GetObjectInfo3Params;
import workspace.GetObjectInfo3Results;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class MainObjectProcessor {
    private File rootTempDir;
    private WSStatusEventReconstructor wsEventReconstructor;
    private WorkspaceClient wsClient;
    private StatusEventStorage eventStorage;
    private AccessGroupProvider accessGroupProvider;
    private ObjectStatusEventQueue queue;
    private Thread mainRunner;
    private SystemStorage systemStorage;
    private IndexingStorage indexingStorage;
    private RelationStorage relationStorage;
    private LineLogger logger;
    private Set<String> admins;
    //TODO RECON remove when reconstructor is replaced by event feed
    private final boolean runWorkspaceEventReconstructor;
    
    public MainObjectProcessor(
            final URL wsURL,
            final AuthToken kbaseIndexerToken,
            final String mongoHost, 
            final int mongoPort,
            final String mongoDbName,
            final HttpHost esHost,
            final String esUser,
            final String esPassword,
            final String esIndexPrefix,
            final File typesDir,
            final File tempDir,
            final boolean startLifecycleRunner,
            final boolean runWorkspaceEventReconstructor,
            final LineLogger logger,
            final Set<String> admins)
            throws IOException, ObjectParseException {
        /* Some notes for the future - I'd probably change this to take an StatusEventStorage
         * interface rather than constructing it itself. This allows easier swapping out of
         * components and easier testing via component mocks.
         * Same for IndexingStorage (now ElasticIndexingStorage) and SystemStorage.
         * I'd also make an interface for retrieving data and pass in a mapping from
         * storageCode to the interface, so allow indexing multiple data sources. 
         * Currently it looks like only the WS is supported and adding other sources might be a 
         * bit tricky.
         */
        this.runWorkspaceEventReconstructor = runWorkspaceEventReconstructor;
        this.logger = logger;
        this.rootTempDir = tempDir;
        this.admins = admins == null ? Collections.emptySet() : admins;
        //TODO NOW mongo auth
        @SuppressWarnings("resource") // can't close it or the connection shuts down
        // may need a shut down listener to ensure the client shuts down, but probably unnecessary
        final MongoClient cli = new MongoClient(new ServerAddress(mongoHost + ":" + mongoPort));
        MongoDBStatusEventStorage storage = new MongoDBStatusEventStorage(
                cli.getDatabase(mongoDbName));
        eventStorage = storage;
        WSStatusEventReconstructorImpl reconstructor = new WSStatusEventReconstructorImpl(
                wsURL, kbaseIndexerToken, eventStorage);
        wsClient = reconstructor.wsClient();
        // 50k simultaneous users * 1000 group ids each seems like plenty = 50M ints in memory
        accessGroupProvider = new AccessGroupCache(new WorkspaceAccessGroupProvider(wsClient),
                30, 50000 * 1000);
        wsEventReconstructor = reconstructor;
        reconstructor.registerListener(storage);
        if (logger != null) {
            reconstructor.registerListener(new StatusEventListener() {
                @Override
                public void objectStatusChanged(List<ObjectStatusEvent> events) 
                        throws IOException {
                    for (ObjectStatusEvent obj : events){
                        logger.logInfo("[Reconstructor] objectStatusChanged: " + obj);
                    }
                }
                @Override
                public void groupStatusChanged(List<AccessGroupStatus> newStatuses)
                        throws IOException {
                    for (AccessGroupStatus obj : newStatuses){
                        logger.logInfo("[Reconstructor] groupStatusChanged: " + obj);
                    }
                }
                
                @Override
                public void groupPermissionsChanged(List<AccessGroupStatus> newStatuses)
                        throws IOException {
                    /*for (AccessGroupStatus obj : newStatuses){
                        logger.logInfo("[Reconstructor] groupPermissionsChanged: " + obj);
                    }*/
                }
            });
        }
        queue = new ObjectStatusEventQueue(eventStorage);
        systemStorage = new DefaultSystemStorage(typesDir);
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHost, 
                getTempSubDir("esbulk"));
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexingStorage = esStorage;
        relationStorage = new DefaultRelationStorage();
        // We switch this flag off in tests 
        if (startLifecycleRunner) {
            startLifecycleRunner();
        }
    }
    
    /**
     * For tests only !!!
     */
    public MainObjectProcessor(URL wsURL, AuthToken kbaseIndexerToken, 
            HttpHost esHost, String esUser, String esPassword,
            String esIndexPrefix, File typesDir, File tempDir, LineLogger logger) 
                    throws IOException, ObjectParseException, UnauthorizedException {
        this.runWorkspaceEventReconstructor = true;
        this.rootTempDir = tempDir;
        this.logger = logger;
        this.admins = Collections.emptySet();
        wsClient = new WorkspaceClient(wsURL, kbaseIndexerToken);
        wsClient.setIsInsecureHttpConnectionAllowed(true);         
        systemStorage = new DefaultSystemStorage(typesDir);
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHost, 
                getTempSubDir("esbulk"));
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexingStorage = esStorage;
        relationStorage = new DefaultRelationStorage();
    }
    
    private File getTempSubDir(String subName) {
        File ret = new File(rootTempDir, subName);
        if (!ret.exists()) {
            ret.mkdirs();
        }
        return ret;
    }
    
    public void startLifecycleRunner() {
        if (mainRunner != null) {
            throw new IllegalStateException("Lifecycle runner was already started");
        }
        mainRunner = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int iter = 0; !Thread.currentThread().isInterrupted(); iter++) {
                        try {
                            performOneTick(iter % 10 == 0);
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            logError(e);
                        }
                    }
                } finally {
                    mainRunner = null;
                }
            }
        });
        mainRunner.start();
    }
    
    private void logError(Throwable e) {
        String codePlace = e.getStackTrace().length == 0 ? "<not-available>" : 
            e.getStackTrace()[0].toString();
        if (logger != null) {
            logger.logError("Error in Lifecycle runner: " + e + ", " + codePlace);
            logger.logError(e);
        }
    }
    
    public boolean stopLifecycleRunner() {
        if (mainRunner == null) {
            return false;
        }
        if (mainRunner.isInterrupted()) {
            throw new IllegalStateException("Lifecycle Runner can not be stopped twice");
        }
        mainRunner.interrupt();
        // Let's check every 100 ms during 1 minute at most.
        for (int i = 0; i < 600; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
            if (mainRunner == null) {
                return true;
            }
        }
        throw new IllegalStateException("Failed to stop Lifecycle Runner");
    }
    
    public void performOneTick(boolean permissions)
            throws IOException, JsonClientException, ObjectParseException {
        if (runWorkspaceEventReconstructor) {
            Set<Long> excludeWsIds = Collections.emptySet();
            //Set<Long> excludeWsIds = new LinkedHashSet<>(Arrays.asList(10455L));
            wsEventReconstructor.processWorkspaceObjects(15L, PresenceType.PRESENT);
            wsEventReconstructor.processWorkspaceObjects(AccessType.PRIVATE, PresenceType.PRESENT, 
                    excludeWsIds);
            if (permissions) {
                wsEventReconstructor.processWorkspacePermissions(AccessType.ALL, null);
            }
        }
        // Seems like this shouldn't be source specific. It should handle all event sources.
        ObjectStatusEventIterator iter = queue.iterator("WS");
        while (iter.hasNext()) {
            //TODO NOW markAsVisited is called for every sub event, which is pointless. It should be called only when all sub events are processed.
            final ObjectStatusEvent preEvent = iter.next();
            for (final ObjectStatusEvent ev: getEventHandler(preEvent).expand(preEvent)) {
                final String type = ev.getStorageObjectType();
                if (type != null && !isStorageTypeSupported(type)) {
                    if (logger != null) {
                        logger.logInfo("[Indexer] skipping " + ev.getEventType() + ", " + 
                                type + ", " + ev.toGUID());
                    }
                    iter.markAsVisitied(false);
                    continue;
                }
                if (logger != null) {
                    logger.logInfo("[Indexer] processing " + ev.getEventType() + ", " + 
                            type + ", " + ev.toGUID() + "...");
                }
                long time = System.currentTimeMillis();
                try {
                    processOneEvent(ev);
                } catch (Exception e) {
                    //TODO NOW with event expansion, this doesn't really work right.
                    // Will skip the sub event - should follow one of 3 strategies - retry, turn off the event handler, or ignore the parent event
                    logError(e);
                    iter.markAsVisitied(false);
                    continue;
                }
                iter.markAsVisitied(true);
                if (logger != null) {
                    logger.logInfo("[Indexer]   (total time: " +
                            (System.currentTimeMillis() - time) + "ms.)");
                }
            }
        }
    }
    
    private EventHandler getEventHandler(final ObjectStatusEvent ev) {
        return getEventHandler(ev.getStorageCode());
    }
    
    private EventHandler getEventHandler(final GUID guid) {
        return getEventHandler(guid.getStorageCode());
    }
    
    private EventHandler getEventHandler(final String storageCode) {
        //TODO HANDLERS should pull from a hashmap of event type -> handler
        if (!"WS".equals(storageCode)) {
            //TODO EXP need to make this an error such that the event is not reprocessed
            throw new IllegalStateException("Only WS events are currently supported");
        }
        return new WorkspaceEventHandler(wsClient);
    }

    public void processOneEvent(ObjectStatusEvent ev) 
            throws IOException, JsonClientException, ObjectParseException {
        switch (ev.getEventType()) {
        case CREATED:
        case NEW_VERSION:
            GUID pguid = ev.toGUID();
            boolean indexed = indexingStorage.checkParentGuidsExist(null, new LinkedHashSet<>(
                    Arrays.asList(pguid))).get(pguid);
            if (indexed) {
                logger.logInfo("[Indexer]   skipping " + pguid + " creation (already indexed)");
                // TODO: we should fix public access for all sub-objects too !!!
                if (ev.isGlobalAccessed()) {
                    publish(pguid);
                } else {
                    unpublish(pguid);
                }
            } else {
                indexObject(pguid, ev.getStorageObjectType(), ev.getTimestamp(),
                        ev.isGlobalAccessed(), null, new LinkedList<>());
            }
            break;
        case DELETED:
            unshare(ev.toGUID(), ev.getAccessGroupId());
            break;
        case DELETE_ALL_VERSIONS:
            unshareAllVersions(ev.toGUID());
            break;
        case UNDELETE_ALL_VERSIONS:
            shareAllVersions(ev.toGUID());
            break;
        case SHARED:
            share(ev.toGUID(), ev.getTargetAccessGroupId());
            break;
        case UNSHARED:
            unshare(ev.toGUID(), ev.getTargetAccessGroupId());
            break;
        case RENAME_ALL_VERSIONS:
            renameAllVersions(ev.toGUID(), ev.getNewName());
            break;
        case PUBLISH_ALL_VERSIONS:
            publishAllVersions(ev.toGUID());
            break;
        case UNPUBLISH_ALL_VERSIONS:
            unpublishAllVersions(ev.toGUID());
            break;
        default:
            throw new IllegalStateException("Unsupported event type: " + ev.getEventType());
        }
    }

    public boolean isStorageTypeSupported(String storageObjectType) throws IOException {
        return systemStorage.listObjectTypesByStorageObjectType(storageObjectType) != null;
    }
    
    private void indexObject(GUID guid, String storageObjectType, Long timestamp, boolean isPublic,
            ObjectLookupProvider indexLookup, List<GUID> objectRefPath) 
                    throws IOException, JsonClientException, ObjectParseException {
        long t1 = System.currentTimeMillis();
        File tempFile = ObjectParser.prepareTempFile(getTempSubDir(guid.getStorageCode()));
        if (indexLookup == null) {
            indexLookup = new MOPLookupProvider();
        }
        try {
            objectRefPath.add(guid);
            final EventHandler handler = getEventHandler(guid);
            final SourceData obj = handler.load(objectRefPath, tempFile.toPath());
            if (logger != null) {
                long loadTime = System.currentTimeMillis() - t1;
                logger.logInfo("[Indexer]   " + guid + ", loading time: " + loadTime + " ms.");
                logger.timeStat(guid, loadTime, 0, 0);
            }
            List<ObjectTypeParsingRules> parsingRules = 
                    systemStorage.listObjectTypesByStorageObjectType(storageObjectType);
            for (ObjectTypeParsingRules rule : parsingRules) {
                long t2 = System.currentTimeMillis();
                String parentJson = null;
                try (JsonParser jts = obj.getData().getPlacedStream()) {
                    parentJson = ObjectParser.extractParentFragment(rule, jts);
                }
                Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, guid, rule, 
                        systemStorage, relationStorage);
                Map<GUID, ParsedObject> guidToObj = new LinkedHashMap<>();
                for (GUID subGuid : guidToJson.keySet()) {
                    String json = guidToJson.get(subGuid);
                    ParsedObject prsObj = KeywordParser.extractKeywords(rule.getGlobalObjectType(),
                            json, parentJson, rule.getIndexingRules(), indexLookup, 
                            objectRefPath);
                    guidToObj.put(subGuid, prsObj);
                }
                long parsingTime = System.currentTimeMillis() - t2;
                if (logger != null) {
                    logger.logInfo("[Indexer]   " + rule.getGlobalObjectType() + ", parsing " +
                            "time: " + parsingTime + " ms.");
                }
                long t3 = System.currentTimeMillis();
                if (timestamp == null) {
                    timestamp = System.currentTimeMillis();
                }
                indexingStorage.indexObjects(rule.getGlobalObjectType(), obj,
                        timestamp, parentJson, guid, guidToObj, isPublic, rule.getIndexingRules());
                if (logger != null) {
                    long indexTime = System.currentTimeMillis() - t3;
                    logger.logInfo("[Indexer]   " + rule.getGlobalObjectType() + ", indexing " +
                            "time: " + indexTime + " ms.");
                    logger.timeStat(guid, 0, parsingTime, indexTime);
                }
            }
        } finally {
            tempFile.delete();
        }
    }
    
    public void share(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId, 
                false);
    }
    
    public void shareAllVersions(final GUID guid) throws IOException {
        indexingStorage.undeleteAllVersions(guid);
    }

    public void unshare(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
    }

    public void unshareAllVersions(final GUID guid) throws IOException {
        indexingStorage.deleteAllVersions(guid);
    }

    public void publish(GUID guid) throws IOException {
        indexingStorage.publishObjects(new LinkedHashSet<>(Arrays.asList(guid)));
    }
    
    public void publishAllVersions(final GUID guid) throws IOException {
        indexingStorage.publishAllVersions(guid);
        //TODO DP need to handle objects in datapalette
    }

    public void unpublish(GUID guid) throws IOException {
        indexingStorage.unpublishObjects(new LinkedHashSet<>(Arrays.asList(guid)));
    }
    
    public void unpublishAllVersions(final GUID guid) throws IOException {
        indexingStorage.unpublishAllVersions(guid);
        //TODO DP need to handle objects in datapalette
    }
    
    private void renameAllVersions(final GUID guid, final String newName) throws IOException {
        indexingStorage.setNameOnAllObjectVersions(guid, newName);
    }

    public IndexingStorage getIndexingStorage(String objectType) {
        return indexingStorage;
    }
    
    private static boolean toBool(Long value) {
        return value != null && value == 1L;
    }

    private static boolean toBool(Long value, boolean defaultRet) {
        if (value == null) {
            return defaultRet;
        }
        return value == 1L;
    }

    private static Integer toInteger(Long value) {
        return value == null ? null : (int)(long)value;
    }

    private static GUID toGUID(String value) {
        return value == null ? null : new GUID(value);
    }

    private kbaserelationengine.search.MatchValue toSearch(MatchValue mv, String source) {
        if (mv == null) {
            return null;
        }
        if (mv.getValue() != null) {
            return new kbaserelationengine.search.MatchValue(mv.getValue());
        }
        if (mv.getIntValue() != null) {
            return new kbaserelationengine.search.MatchValue(toInteger(mv.getIntValue()));
        }
        if (mv.getDoubleValue() != null) {
            return new kbaserelationengine.search.MatchValue(mv.getDoubleValue());
        }
        if (mv.getBoolValue() != null) {
            return new kbaserelationengine.search.MatchValue(toBool(mv.getBoolValue()));
        }
        if (mv.getMinInt() != null || mv.getMaxInt() != null) {
            return new kbaserelationengine.search.MatchValue(toInteger(mv.getMinInt()),
                    toInteger(mv.getMaxInt()));
        }
        if (mv.getMinDate() != null || mv.getMaxDate() != null) {
            return new kbaserelationengine.search.MatchValue(mv.getMinDate(), mv.getMaxDate());
        }
        if (mv.getMinDouble() != null || mv.getMaxDouble() != null) {
            return new kbaserelationengine.search.MatchValue(mv.getMinDouble(), mv.getMaxDouble());
        }
        throw new IllegalStateException("Unsupported " + source + " filter: " + mv);
    }
    
    private kbaserelationengine.search.MatchFilter toSearch(MatchFilter mf) {
        kbaserelationengine.search.MatchFilter ret = 
                new kbaserelationengine.search.MatchFilter()
                .withFullTextInAll(mf.getFullTextInAll())
                .withAccessGroupId(toInteger(mf.getAccessGroupId()))
                .withObjectName(mf.getObjectName())
                .withParentGuid(toGUID(mf.getParentGuid()))
                .withTimestamp(toSearch(mf.getTimestamp(), "timestamp"));
        if (mf.getLookupInKeys() != null) {
            Map<String, kbaserelationengine.search.MatchValue> keys =
                    new LinkedHashMap<String, kbaserelationengine.search.MatchValue>();
            for (String key : mf.getLookupInKeys().keySet()) {
                keys.put(key, toSearch(mf.getLookupInKeys().get(key), key));
            }
            ret.withLookupInKeys(keys);
        }
        return ret;
    }

    private kbaserelationengine.search.AccessFilter toSearch(AccessFilter af, String user)
            throws IOException {
        List<Integer> accessGroupIds;
        if (toBool(af.getWithPrivate(), true)) {
            accessGroupIds = accessGroupProvider.findAccessGroupIds(user);
        } else {
            accessGroupIds = Collections.emptyList();
        }
        return new kbaserelationengine.search.AccessFilter()
                .withPublic(toBool(af.getWithPublic()))
                .withAllHistory(toBool(af.getWithAllHistory()))
                .withAccessGroups(new LinkedHashSet<>(accessGroupIds))
                .withAdmin(admins.contains(user));
    }
    
    private kbaserelationengine.search.SortingRule toSearch(SortingRule sr) {
        if (sr == null) {
            return null;
        }
        kbaserelationengine.search.SortingRule ret = new kbaserelationengine.search.SortingRule();
        ret.isTimestamp = toBool(sr.getIsTimestamp());
        ret.isObjectName = toBool(sr.getIsObjectName());
        ret.keyName = sr.getKeyName();
        ret.ascending = !toBool(sr.getDescending());
        return ret;
    }

    private SortingRule fromSearch(kbaserelationengine.search.SortingRule sr) {
        if (sr == null) {
            return null;
        }
        SortingRule ret = new SortingRule();
        if (sr.isTimestamp) {
            ret.withIsTimestamp(1L);
        } else if (sr.isObjectName) {
            ret.withIsObjectName(1L);
        } else {
            ret.withKeyName(sr.keyName);
        }
        ret.withDescending(sr.ascending ? 0L : 1L);
        return ret;
    }

    private kbaserelationengine.search.Pagination toSearch(Pagination pg) {
        return pg == null ? null : new kbaserelationengine.search.Pagination(
                toInteger(pg.getStart()), toInteger(pg.getCount()));
    }

    private Pagination fromSearch(kbaserelationengine.search.Pagination pg) {
        return pg == null ? null : new Pagination().withStart((long)pg.start)
                .withCount((long)pg.count);
    }

    private kbaserelationengine.search.PostProcessing toSearch(PostProcessing pp) {
        kbaserelationengine.search.PostProcessing ret = 
                new kbaserelationengine.search.PostProcessing();
        if (pp == null) {
            ret.objectInfo = true;
            ret.objectData = true;
            ret.objectKeys = true;
        } else {
            boolean idsOnly = toBool(pp.getIdsOnly());
            ret.objectInfo = !(toBool(pp.getSkipInfo()) || idsOnly);
            ret.objectData = !(toBool(pp.getSkipData()) || idsOnly);
            ret.objectKeys = !(toBool(pp.getSkipKeys()) || idsOnly);
        }
        return ret;
    }
    
    private kbaserelationengine.ObjectData fromSearch(kbaserelationengine.search.ObjectData od) {
        kbaserelationengine.ObjectData ret = new kbaserelationengine.ObjectData();
        ret.withGuid(od.guid.toString());
        if (od.parentGuid != null) {
            ret.withParentGuid(od.parentGuid.toString());
        }
        if (od.timestamp > 0) {
            ret.withTimestamp(od.timestamp);
        }
        if (od.data != null) {
            ret.withData(new UObject(od.data));
        }
        if (od.parentData != null) {
            ret.withParentData(new UObject(od.parentData));
        }
        ret.withObjectName(od.objectName);
        ret.withKeyProps(od.keyProps);
        return ret;
    }
    
    public SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception {
        long t1 = System.currentTimeMillis();
        kbaserelationengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        kbaserelationengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        Map<String, Integer> ret = indexingStorage.searchTypes(matchFilter, accessFilter);
        return new SearchTypesOutput().withTypeToCount(ret.keySet().stream().collect(
                Collectors.toMap(Function.identity(), c -> (long)(int)ret.get(c))))
                .withSearchTime(System.currentTimeMillis() - t1);
    }
    
    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user) 
            throws Exception {
        long t1 = System.currentTimeMillis();
        kbaserelationengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        List<kbaserelationengine.search.SortingRule> sorting = null;
        if (params.getSortingRules() != null) {
            sorting = params.getSortingRules().stream().map(this::toSearch).collect(
                    Collectors.toList());
        }
        kbaserelationengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        kbaserelationengine.search.Pagination pagination = toSearch(params.getPagination());
        kbaserelationengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        FoundHits hits = indexingStorage.searchObjects(params.getObjectType(), matchFilter, 
                sorting, accessFilter, pagination, postProcessing);
        SearchObjectsOutput ret = new SearchObjectsOutput();
        ret.withPagination(fromSearch(hits.pagination));
        ret.withSortingRules(hits.sortingRules.stream().map(this::fromSearch).collect(
                Collectors.toList()));
        if (hits.objects == null) {
            ret.withObjects(hits.guids.stream().map(guid -> new kbaserelationengine.ObjectData().
                    withGuid(guid.toString())).collect(Collectors.toList()));
        } else {
            ret.withObjects(hits.objects.stream().map(this::fromSearch).collect(
                    Collectors.toList()));
        }
        ret.withTotal((long)hits.total);
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }

    public GetObjectsOutput getObjects(GetObjectsInput params, String user) throws Exception {
        long t1 = System.currentTimeMillis();
        Set<GUID> guids = new LinkedHashSet<>();
        for (String guid : params.getGuids()) {
            guids.add(new GUID(guid));
        }
        kbaserelationengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        List<kbaserelationengine.search.ObjectData> objs = indexingStorage.getObjectsByIds(
                guids, postProcessing);
        GetObjectsOutput ret = new GetObjectsOutput().withObjects(objs.stream()
                .map(this::fromSearch).collect(Collectors.toList()));
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }
    
    public Map<String, TypeDescriptor> listTypes(String uniqueType) throws Exception {
        Map<String, TypeDescriptor> ret = new LinkedHashMap<>();
        for (ObjectTypeParsingRules otpr : systemStorage.listObjectTypes()) {
            String typeName = otpr.getGlobalObjectType();
            if (uniqueType != null && !uniqueType.equals(typeName)) {
                continue;
            }
            String uiTypeName = otpr.getUiTypeName();
            if (uiTypeName == null) {
                uiTypeName = guessUIName(typeName);
            }
            List<KeyDescription> keys = new ArrayList<>();
            for (IndexingRules ir : otpr.getIndexingRules()) {
                if (ir.isNotIndexed()) {
                    continue;
                }
                String keyName = KeywordParser.getKeyName(ir);
                String uiKeyName = ir.getUiName();
                if (uiKeyName == null) {
                    uiKeyName = guessUIName(keyName);
                }
                String keyValueType = ir.getKeywordType();
                if (keyValueType == null) {
                    keyValueType = "string";
                }
                long hidden = ir.isUiHidden() ? 1L : 0L;
                KeyDescription kd = new KeyDescription().withKeyName(keyName)
                        .withKeyUiTitle(uiKeyName).withKeyValueType(keyValueType)
                        .withKeyValueType(keyValueType).withHidden(hidden)
                        .withLinkKey(ir.getUiLinkKey());
                keys.add(kd);
            }
            TypeDescriptor td = new TypeDescriptor().withTypeName(typeName)
                    .withTypeUiTitle(uiTypeName).withKeys(keys);
            ret.put(typeName, td);
        }
        return ret;
    }

    private static String guessUIName(String id) {
        return id.substring(0, 1).toUpperCase() + id.substring(1);
    }
    
    private class MOPLookupProvider implements ObjectLookupProvider {
        private Map<String, String> refResolvingCache = new LinkedHashMap<>();
        private Map<GUID, kbaserelationengine.search.ObjectData> objLookupCache =
                new LinkedHashMap<>();
        private Map<GUID, String> guidToTypeCache = new LinkedHashMap<>();
        
        @Override
        public Set<String> resolveWorkspaceRefs(List<GUID> callerRefPath, Set<String> refs)
                throws IOException {
            Set<String> ret = new LinkedHashSet<>();
            Set<String> refsToResolve = new LinkedHashSet<>();
            for (String ref : refs) {
                if (refResolvingCache.containsKey(ref)) {
                    ret.add(refResolvingCache.get(ref));
                } else {
                    refsToResolve.add(ref);
                }
            }
            String refPrefix = callerRefPath == null || callerRefPath.isEmpty() ? "" :
                WorkspaceEventHandler.toWSRefPath(callerRefPath) + ";";
            if (refsToResolve.size() > 0) {
                try {
                    List<ObjectSpecification> getInfoInput = refs.stream().map(
                            ref -> new ObjectSpecification().withRef(refPrefix + ref)).collect(
                                    Collectors.toList());
                    //TODO HANDLER move this to the handler api
                    final Map<String, Object> command = new HashMap<>();
                    command.put("command", "getObjectInfo");
                    command.put("params", new GetObjectInfo3Params().withObjects(getInfoInput));
                    List<ObjectStatusEvent> events = wsClient.administer(new UObject(command))
                            .asClassInstance(GetObjectInfo3Results.class)
                            .getInfos().stream().map(info -> new ObjectStatusEvent("", "WS", 
                                    (int)(long)info.getE7(), "" +info.getE1(), 
                                    (int)(long)info.getE5(), null, null,
                                    Util.DATE_PARSER.parseDateTime(info.getE4()).getMillis(),
                                    info.getE3().split("-")[0],
                                    ObjectStatusEventType.CREATED, false)).collect(
                                            Collectors.toList());
                    for (int pos = 0; pos < getInfoInput.size(); pos++) {
                        String origRef = getInfoInput.get(pos).getRef();
                        ObjectStatusEvent ev = events.get(pos);
                        GUID pguid = ev.toGUID();
                        String resolvedRef = pguid.getAccessGroupId() + "/" + 
                                pguid.getAccessGroupObjectId() + "/" + pguid.getVersion();
                        boolean indexed = indexingStorage.checkParentGuidsExist(null, 
                                new LinkedHashSet<>(Arrays.asList(pguid))).get(pguid);
                        if (!indexed) {
                            indexObject(pguid, ev.getStorageObjectType(), ev.getTimestamp(), false,
                                    this, callerRefPath);
                        }
                        refResolvingCache.put(origRef, resolvedRef);
                        ret.add(resolvedRef);
                        if (!origRef.equals(resolvedRef)) {
                            refResolvingCache.put(resolvedRef, resolvedRef);
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }
            return ret;
        }
        
        @Override
        public Map<GUID, kbaserelationengine.search.ObjectData> lookupObjectsByGuid(
                Set<GUID> guids) throws IOException {
            Map<GUID, kbaserelationengine.search.ObjectData> ret = new LinkedHashMap<>();
            Set<GUID> guidsToLoad = new LinkedHashSet<>();
            for (GUID guid : guids) {
                if (objLookupCache.containsKey(guid)) {
                    ret.put(guid, objLookupCache.get(guid));
                } else {
                    guidsToLoad.add(guid);
                }
            }
            if (guidsToLoad.size() > 0) {
                kbaserelationengine.search.PostProcessing pp = 
                        new kbaserelationengine.search.PostProcessing();
                pp.objectData = false;
                pp.objectKeys = true;
                pp.objectInfo = true;
                List<kbaserelationengine.search.ObjectData> objList = 
                        indexingStorage.getObjectsByIds(guidsToLoad, pp);
                Map<GUID, kbaserelationengine.search.ObjectData> loaded = 
                        objList.stream().collect(Collectors.toMap(od -> od.guid, 
                                Function.identity()));
                objLookupCache.putAll(loaded);
                ret.putAll(loaded);
            }
            return ret;
        }
        
        @Override
        public ObjectTypeParsingRules getTypeDescriptor(String type) {
            try {
                return systemStorage.getObjectType(type);
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
        
        @Override
        public Map<GUID, String> getTypesForGuids(Set<GUID> guids) throws IOException {
            Map<GUID, String> ret = new LinkedHashMap<>();
            Set<GUID> guidsToLoad = new LinkedHashSet<>();
            for (GUID guid : guids) {
                if (guidToTypeCache.containsKey(guid)) {
                    ret.put(guid, guidToTypeCache.get(guid));
                } else {
                    guidsToLoad.add(guid);
                }
            }
            if (guidsToLoad.size() > 0) {
                kbaserelationengine.search.PostProcessing pp = 
                        new kbaserelationengine.search.PostProcessing();
                pp.objectData = false;
                pp.objectKeys = false;
                pp.objectInfo = true;
                Map<GUID, String> loaded = indexingStorage.getObjectsByIds(guids, pp).stream()
                        .collect(Collectors.toMap(od -> od.guid, od -> od.type));
                guidToTypeCache.putAll(loaded);
                ret.putAll(loaded);
            }
            return ret;
        }
    }
}
