package kbasesearchengine.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.AccessFilter;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.KeyDescription;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.MatchValue;
import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.PostProcessing;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SortingRule;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.AccessGroupCache;
import kbasesearchengine.events.AccessGroupProvider;
import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.ObjectStatusEventType;
import kbasesearchengine.events.WorkspaceAccessGroupProvider;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.parse.KeywordParser.ObjectLookupProvider;
import kbasesearchengine.queue.ObjectStatusEventIterator;
import kbasesearchengine.queue.ObjectStatusEventQueue;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeStorage;
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
    private WorkspaceClient wsClient;
    private AccessGroupProvider accessGroupProvider;
    private ObjectStatusEventQueue queue;
    private Thread mainRunner;
    private final TypeStorage typeStorage;
    private IndexingStorage indexingStorage;
    private LineLogger logger;
    private Set<String> admins;
    
    public MainObjectProcessor(
            final URL wsURL,
            final AuthToken kbaseIndexerToken,
            final MongoDatabase db,
            final HttpHost esHost,
            final String esUser,
            final String esPassword,
            final String esIndexPrefix,
            final TypeStorage typeStorage,
            final File tempDir,
            final boolean startLifecycleRunner,
            final LineLogger logger,
            final Set<String> admins)
            throws IOException, ObjectParseException, UnauthorizedException {
        /* Some notes for the future - I'd probably change this to take an StatusEventStorage
         * interface rather than constructing it itself. This allows easier swapping out of
         * components and easier testing via component mocks.
         * Same for IndexingStorage (now ElasticIndexingStorage) and SystemStorage.
         * I'd also make an interface for retrieving data and pass in a mapping from
         * storageCode to the interface, so allow indexing multiple data sources. 
         * Currently it looks like only the WS is supported and adding other sources might be a 
         * bit tricky.
         */
        this.logger = logger;
        this.rootTempDir = tempDir;
        this.admins = admins == null ? Collections.emptySet() : admins;
        
        MongoDBStatusEventStorage storage = new MongoDBStatusEventStorage(db);
        wsClient = new WorkspaceClient(wsURL, kbaseIndexerToken);
        wsClient.setIsInsecureHttpConnectionAllowed(true); //TODO SEC only do if http
        // 50k simultaneous users * 1000 group ids each seems like plenty = 50M ints in memory
        accessGroupProvider = new AccessGroupCache(new WorkspaceAccessGroupProvider(wsClient),
                30, 50000 * 1000);
        queue = new ObjectStatusEventQueue(storage);
        this.typeStorage = typeStorage;
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHost, 
                getTempSubDir("esbulk"));
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexingStorage = esStorage;
        // We switch this flag off in tests 
        if (startLifecycleRunner) {
            startLifecycleRunner();
        }
    }
    
    /**
     * For tests only !!!
     */
    public MainObjectProcessor(
            final URL wsURL,
            final AuthToken kbaseIndexerToken, 
            final HttpHost esHost,
            final String esUser,
            final String esPassword,
            final String esIndexPrefix,
            final TypeStorage typeStorage,
            final File tempDir,
            final LineLogger logger) 
            throws IOException, ObjectParseException, UnauthorizedException {
        this.rootTempDir = tempDir;
        this.logger = logger;
        this.admins = Collections.emptySet();
        wsClient = new WorkspaceClient(wsURL, kbaseIndexerToken);
        wsClient.setIsInsecureHttpConnectionAllowed(true);
        this.typeStorage = typeStorage;
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHost, 
                getTempSubDir("esbulk"));
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexingStorage = esStorage;
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
        // Seems like this shouldn't be source specific. It should handle all event sources.
        ObjectStatusEventIterator iter = queue.iterator("WS");
        while (iter.hasNext()) {
            //TODO NOW markAsVisited is called for every sub event, which is pointless. It should be called only when all sub events are processed.
            final ObjectStatusEvent preEvent = iter.next();
            for (final ObjectStatusEvent ev: getEventHandler(preEvent).expand(preEvent)) {
                final StorageObjectType type = ev.getStorageObjectType();
                if (type != null && !isStorageTypeSupported(type)) {
                    if (logger != null) {
                        logger.logInfo("[Indexer] skipping " + ev.getEventType() + ", " + 
                                toLogString(type) + ev.toGUID());
                    }
                    iter.markAsVisitied(false);
                    continue;
                }
                if (logger != null) {
                    logger.logInfo("[Indexer] processing " + ev.getEventType() + ", " + 
                            toLogString(type) + ev.toGUID() + "...");
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
    
    private String toLogString(final StorageObjectType type) {
        if (type == null) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(type.getStorageCode());
        sb.append(":");
        sb.append(type.getType());
        if (type.getVersion().isPresent()) {
            sb.append("-");
            sb.append(type.getVersion().get());
        }
        sb.append(", ");
        return sb.toString();
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

    public boolean isStorageTypeSupported(final StorageObjectType storageObjectType)
            throws IOException {
        return !typeStorage.listObjectTypesByStorageObjectType(storageObjectType).isEmpty();
    }
    
    private void indexObject(
            final GUID guid,
            final StorageObjectType storageObjectType,
            Long timestamp,
            final boolean isPublic,
            ObjectLookupProvider indexLookup,
            final List<GUID> objectRefPath) 
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
                    typeStorage.listObjectTypesByStorageObjectType(storageObjectType);
            for (ObjectTypeParsingRules rule : parsingRules) {
                long t2 = System.currentTimeMillis();
                String parentJson = null;
                try (JsonParser jts = obj.getData().getPlacedStream()) {
                    parentJson = ObjectParser.extractParentFragment(rule, jts);
                }
                Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, guid, rule);
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

    private kbasesearchengine.search.MatchValue toSearch(MatchValue mv, String source) {
        if (mv == null) {
            return null;
        }
        if (mv.getValue() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getValue());
        }
        if (mv.getIntValue() != null) {
            return new kbasesearchengine.search.MatchValue(toInteger(mv.getIntValue()));
        }
        if (mv.getDoubleValue() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getDoubleValue());
        }
        if (mv.getBoolValue() != null) {
            return new kbasesearchengine.search.MatchValue(toBool(mv.getBoolValue()));
        }
        if (mv.getMinInt() != null || mv.getMaxInt() != null) {
            return new kbasesearchengine.search.MatchValue(toInteger(mv.getMinInt()),
                    toInteger(mv.getMaxInt()));
        }
        if (mv.getMinDate() != null || mv.getMaxDate() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getMinDate(), mv.getMaxDate());
        }
        if (mv.getMinDouble() != null || mv.getMaxDouble() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getMinDouble(), mv.getMaxDouble());
        }
        throw new IllegalStateException("Unsupported " + source + " filter: " + mv);
    }
    
    private kbasesearchengine.search.MatchFilter toSearch(MatchFilter mf) {
        kbasesearchengine.search.MatchFilter ret = 
                new kbasesearchengine.search.MatchFilter()
                .withFullTextInAll(mf.getFullTextInAll())
                .withAccessGroupId(toInteger(mf.getAccessGroupId()))
                .withObjectName(mf.getObjectName())
                .withParentGuid(toGUID(mf.getParentGuid()))
                .withTimestamp(toSearch(mf.getTimestamp(), "timestamp"));
        if (mf.getLookupInKeys() != null) {
            Map<String, kbasesearchengine.search.MatchValue> keys =
                    new LinkedHashMap<String, kbasesearchengine.search.MatchValue>();
            for (String key : mf.getLookupInKeys().keySet()) {
                keys.put(key, toSearch(mf.getLookupInKeys().get(key), key));
            }
            ret.withLookupInKeys(keys);
        }
        return ret;
    }

    private kbasesearchengine.search.AccessFilter toSearch(AccessFilter af, String user)
            throws IOException {
        List<Integer> accessGroupIds;
        if (toBool(af.getWithPrivate(), true)) {
            accessGroupIds = accessGroupProvider.findAccessGroupIds(user);
        } else {
            accessGroupIds = Collections.emptyList();
        }
        return new kbasesearchengine.search.AccessFilter()
                .withPublic(toBool(af.getWithPublic()))
                .withAllHistory(toBool(af.getWithAllHistory()))
                .withAccessGroups(new LinkedHashSet<>(accessGroupIds))
                .withAdmin(admins.contains(user));
    }
    
    private kbasesearchengine.search.SortingRule toSearch(SortingRule sr) {
        if (sr == null) {
            return null;
        }
        kbasesearchengine.search.SortingRule ret = new kbasesearchengine.search.SortingRule();
        ret.isTimestamp = toBool(sr.getIsTimestamp());
        ret.isObjectName = toBool(sr.getIsObjectName());
        ret.keyName = sr.getKeyName();
        ret.ascending = !toBool(sr.getDescending());
        return ret;
    }

    private SortingRule fromSearch(kbasesearchengine.search.SortingRule sr) {
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

    private kbasesearchengine.search.Pagination toSearch(Pagination pg) {
        return pg == null ? null : new kbasesearchengine.search.Pagination(
                toInteger(pg.getStart()), toInteger(pg.getCount()));
    }

    private Pagination fromSearch(kbasesearchengine.search.Pagination pg) {
        return pg == null ? null : new Pagination().withStart((long)pg.start)
                .withCount((long)pg.count);
    }

    private kbasesearchengine.search.PostProcessing toSearch(PostProcessing pp) {
        kbasesearchengine.search.PostProcessing ret = 
                new kbasesearchengine.search.PostProcessing();
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
    
    private kbasesearchengine.ObjectData fromSearch(
            final kbasesearchengine.search.ObjectData od) {
        final kbasesearchengine.ObjectData ret = new kbasesearchengine.ObjectData();
        ret.withGuid(od.guid.toString());
        ret.withObjectProps(new HashMap<>());
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
        addObjectProp(ret, od.creator, "creator");
        addObjectProp(ret, od.copier, "copied");
        addObjectProp(ret, od.module, "module");
        addObjectProp(ret, od.method, "method");
        addObjectProp(ret, od.moduleVersion, "module_ver");
        addObjectProp(ret, od.commitHash, "commmit");
        return ret;
    }
    
    private void addObjectProp(final ObjectData ret, final String prop, final String propkey) {
        if (prop != null) {
            ret.getObjectProps().put(propkey, prop);
        }
    }

    public SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception {
        long t1 = System.currentTimeMillis();
        kbasesearchengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        kbasesearchengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        Map<String, Integer> ret = indexingStorage.searchTypes(matchFilter, accessFilter);
        return new SearchTypesOutput().withTypeToCount(ret.keySet().stream().collect(
                Collectors.toMap(Function.identity(), c -> (long)(int)ret.get(c))))
                .withSearchTime(System.currentTimeMillis() - t1);
    }
    
    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user) 
            throws Exception {
        long t1 = System.currentTimeMillis();
        kbasesearchengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        List<kbasesearchengine.search.SortingRule> sorting = null;
        if (params.getSortingRules() != null) {
            sorting = params.getSortingRules().stream().map(this::toSearch).collect(
                    Collectors.toList());
        }
        kbasesearchengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        kbasesearchengine.search.Pagination pagination = toSearch(params.getPagination());
        kbasesearchengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        FoundHits hits = indexingStorage.searchObjects(params.getObjectType(), matchFilter, 
                sorting, accessFilter, pagination, postProcessing);
        SearchObjectsOutput ret = new SearchObjectsOutput();
        ret.withPagination(fromSearch(hits.pagination));
        ret.withSortingRules(hits.sortingRules.stream().map(this::fromSearch).collect(
                Collectors.toList()));
        if (hits.objects == null) {
            ret.withObjects(hits.guids.stream().map(guid -> new kbasesearchengine.ObjectData().
                    withGuid(guid.toString())).collect(Collectors.toList()));
        } else {
            ret.withObjects(hits.objects.stream().map(this::fromSearch).collect(
                    Collectors.toList()));
        }
        ret.withTotal((long)hits.total);
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }

    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        final long t1 = System.currentTimeMillis();
        final Set<Integer> accessGroupIDs =
                new HashSet<>(accessGroupProvider.findAccessGroupIds(user));
        final Set<GUID> guids = new LinkedHashSet<>();
        for (final String guid : params.getGuids()) {
            final GUID g = new GUID(guid);
            //TODO DP this is a quick fix for now, doesn't take data palettes into account
            if (accessGroupIDs.contains(g.getAccessGroupId())) {
                // don't throw an error, just don't return data
                guids.add(g);
            }
        }
        final kbasesearchengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        final List<kbasesearchengine.search.ObjectData> objs = indexingStorage.getObjectsByIds(
                guids, postProcessing);
        final GetObjectsOutput ret = new GetObjectsOutput().withObjects(objs.stream()
                .map(this::fromSearch).collect(Collectors.toList()));
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }
    
    public Map<String, TypeDescriptor> listTypes(String uniqueType) throws Exception {
        Map<String, TypeDescriptor> ret = new LinkedHashMap<>();
        for (ObjectTypeParsingRules otpr : typeStorage.listObjectTypes()) {
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
        private Map<GUID, kbasesearchengine.search.ObjectData> objLookupCache =
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
                                    WorkspaceEventHandler.DATE_PARSER.parseDateTime(
                                            info.getE4()).getMillis(),
                                    new StorageObjectType("WS", info.getE3().split("-")[0],
                                            Integer.parseInt(
                                                    info.getE3().split("-")[1].split("\\.")[0])),
                                    ObjectStatusEventType.NEW_VERSION, false)).collect(
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
        public Map<GUID, kbasesearchengine.search.ObjectData> lookupObjectsByGuid(
                Set<GUID> guids) throws IOException {
            Map<GUID, kbasesearchengine.search.ObjectData> ret = new LinkedHashMap<>();
            Set<GUID> guidsToLoad = new LinkedHashSet<>();
            for (GUID guid : guids) {
                if (objLookupCache.containsKey(guid)) {
                    ret.put(guid, objLookupCache.get(guid));
                } else {
                    guidsToLoad.add(guid);
                }
            }
            if (guidsToLoad.size() > 0) {
                kbasesearchengine.search.PostProcessing pp = 
                        new kbasesearchengine.search.PostProcessing();
                pp.objectData = false;
                pp.objectKeys = true;
                pp.objectInfo = true;
                List<kbasesearchengine.search.ObjectData> objList = 
                        indexingStorage.getObjectsByIds(guidsToLoad, pp);
                Map<GUID, kbasesearchengine.search.ObjectData> loaded = 
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
                return typeStorage.getObjectType(type);
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
                kbasesearchengine.search.PostProcessing pp = 
                        new kbasesearchengine.search.PostProcessing();
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
