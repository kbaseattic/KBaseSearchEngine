package kbaserelationengine.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.AccessFilter;
import kbaserelationengine.MatchFilter;
import kbaserelationengine.SearchTypesInput;
import kbaserelationengine.SearchTypesOutput;
import kbaserelationengine.common.GUID;
import kbaserelationengine.events.AccessGroupProvider;
import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.events.reconstructor.AccessType;
import kbaserelationengine.events.reconstructor.PresenceType;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructor;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbaserelationengine.events.storage.MongoDBStatusEventStorage;
import kbaserelationengine.events.storage.StatusEventStorage;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.queue.ObjectStatusEventIterator;
import kbaserelationengine.queue.ObjectStatusEventQueue;
import kbaserelationengine.relations.DefaultRelationStorage;
import kbaserelationengine.relations.RelationStorage;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.IndexingStorage;
import kbaserelationengine.system.DefaultSystemStorage;
import kbaserelationengine.system.ObjectTypeParsingRules;
import kbaserelationengine.system.SystemStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import workspace.ObjectData;
import workspace.SetPermissionsParams;
import workspace.WorkspaceClient;

public class MainObjectProcessor {
    private URL wsURL;
    private AuthToken kbaseIndexerToken;
    private File rootTempDir;
    private WSStatusEventReconstructor wsEventReconstructor;
    private StatusEventStorage eventStorage;
    private AccessGroupProvider accessGroupProvider;
    private ObjectStatusEventQueue queue;
    private Thread mainRunner;
    private SystemStorage systemStorage;
    private IndexingStorage indexingStorage;
    private RelationStorage relationStorage;
    private LineLogger logger;
    
    public MainObjectProcessor(URL wsURL, AuthToken kbaseIndexerToken, String mongoHost, 
            int mongoPort, String mongoDbName, HttpHost esHost, String esUser, String esPassword,
            String esIndexPrefix, File typesDir, File tempDir, boolean startLifecycleRunner,
            LineLogger logger) throws IOException, ObjectParseException {
        this.logger = logger;
        this.wsURL = wsURL;
        this.kbaseIndexerToken = kbaseIndexerToken;
        this.rootTempDir = tempDir;
        MongoDBStatusEventStorage storage = new MongoDBStatusEventStorage(mongoHost, mongoPort, mongoDbName);
        eventStorage = storage;
        accessGroupProvider = storage;
        WSStatusEventReconstructorImpl reconstructor = new WSStatusEventReconstructorImpl(
                wsURL, kbaseIndexerToken, eventStorage);
        wsEventReconstructor = reconstructor;
        reconstructor.registerListener(storage);
        if (logger != null) {
            reconstructor.registerListener(new StatusEventListener() {
                @Override
                public void objectStatusChanged(List<ObjectStatusEvent> events) 
                        throws IOException {
                    for (ObjectStatusEvent obj : events){
                        logger.logInfo("[Reconstructor] " + obj);
                    }
                }
                @Override
                public void groupStatusChanged(List<AccessGroupStatus> newStatuses)
                        throws IOException {
                    for (AccessGroupStatus obj : newStatuses){
                        logger.logInfo("[Reconstructor] " + obj);
                    }
                }
                
                @Override
                public void groupPermissionsChanged(List<AccessGroupStatus> newStatuses)
                        throws IOException {
                    for (AccessGroupStatus obj : newStatuses){
                        logger.logInfo("[Reconstructor] " + obj);
                    }
                }
            });
        }
        queue = new ObjectStatusEventQueue(eventStorage);
        systemStorage = new DefaultSystemStorage(wsURL, typesDir);
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
    
    private File getWsLoadTempDir() {
        return getTempSubDir("wsload");
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
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            performOneTick();
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
    
    public void performOneTick()
            throws IOException, JsonClientException, ObjectParseException {
        Set<Long> excludeWsIds = Collections.emptySet();
        //Set<Long> excludeWsIds = new LinkedHashSet<>(Arrays.asList(10455L));
        wsEventReconstructor.processWorkspaceObjects(AccessType.PRIVATE, PresenceType.PRESENT, 
                excludeWsIds);
        ObjectStatusEventIterator iter = queue.iterator("WS");
        while (iter.hasNext()) {
            ObjectStatusEvent ev = iter.next();
            if (!isStorageTypeSupported(ev.getStorageObjectType())) {
                if (logger != null) {
                    logger.logInfo("[Indexer] skipping " + ev.getEventType() + ", " + 
                            ev.getStorageObjectType() + ", " + ev.toGUID());
                }
                iter.markAsVisitied(false);
                continue;
            }
            if (logger != null) {
                logger.logInfo("[Indexer] processing " + ev.getEventType() + ", " + 
                        ev.getStorageObjectType() + ", " + ev.toGUID() + "...");
            }
            long time = System.currentTimeMillis();
            try {
                processOneEvent(ev);
            } catch (Exception e) {
                logError(e);
                iter.markAsVisitied(false);
                continue;
            }
            iter.markAsVisitied(true);
            if (logger != null) {
                logger.logInfo("[Indexer]   (total time: " + (System.currentTimeMillis() - time) + "ms.)");
            }
        }
    }
    
    public void processOneEvent(ObjectStatusEvent ev) 
            throws IOException, JsonClientException, ObjectParseException {
        switch (ev.getEventType()) {
        case CREATED:
        case NEW_VERSION:
            indexObject(ev.toGUID(), ev.getStorageObjectType(), ev.getTimestamp());
            break;
        case DELETED:
            unshare(ev.toGUID(), ev.getAccessGroupId());
            break;
        case SHARED:
            share(ev.toGUID(), ev.getTargetAccessGroupId());
            break;
        case UNSHARED:
            unshare(ev.toGUID(), ev.getTargetAccessGroupId());
            break;
        default:
            throw new IllegalStateException("Unsupported event type: " + ev.getEventType());
        }
    }

    public boolean isStorageTypeSupported(String storageObjectType) throws IOException {
        return systemStorage.listObjectTypesByStorageObjectType(storageObjectType) != null;
    }
    
    public void indexObject(GUID guid, String storageObjectType, Long timestamp) 
            throws IOException, JsonClientException, ObjectParseException {
        long t1 = System.currentTimeMillis();
        File tempFile = ObjectParser.prepareTempFile(getWsLoadTempDir());
        try {
            String objRef = guid.getAccessGroupId() + "/" + guid.getAccessGroupObjectId() + "/" +
                    guid.getVersion();
            ObjectData obj = ObjectParser.loadObject(wsURL, tempFile, kbaseIndexerToken, objRef);
            if (logger != null) {
                logger.logInfo("[Indexer]   " + guid + ", loading time: " + 
                        (System.currentTimeMillis() - t1) + " ms.");
            }
            List<ObjectTypeParsingRules> parsingRules = 
                    systemStorage.listObjectTypesByStorageObjectType(storageObjectType);
            for (ObjectTypeParsingRules rule : parsingRules) {
                long t2 = System.currentTimeMillis();
                String parentJson = null;
                try (JsonParser jts = obj.getData().getPlacedStream()) {
                    parentJson = ObjectParser.extractParentFragment(rule, jts);
                }
                Map<GUID, String> guidToJson = ObjectParser.parseSubObjects(obj, objRef, rule, 
                        systemStorage, relationStorage);
                if (logger != null) {
                    logger.logInfo("[Indexer]   " + rule.getGlobalObjectType() + ", parsing " +
                            "time: " + (System.currentTimeMillis() - t2) + " ms.");
                }
                long t3 = System.currentTimeMillis();
                if (timestamp == null) {
                    timestamp = System.currentTimeMillis();
                }
                indexingStorage.indexObjects(rule.getGlobalObjectType(), obj.getInfo().getE2(), 
                        timestamp, parentJson, obj.getInfo().getE11(), guidToJson, 
                        false, rule.getIndexingRules());
                if (logger != null) {
                    logger.logInfo("[Indexer]   " + rule.getGlobalObjectType() + ", indexing " +
                            "time: " + (System.currentTimeMillis() - t3) + " ms.");
                }
            }
        } finally {
            tempFile.delete();
        }
    }
    
    public void share(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.shareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
    }

    public void unshare(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.unshareObjects(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
    }
    
    public void addWorkspaceToIndex(String wsNameOrId, AuthToken user)
            throws IOException, JsonClientException {
        WorkspaceClient wsClient = new WorkspaceClient(wsURL, user);
        wsClient.setIsInsecureHttpConnectionAllowed(true); 
        SetPermissionsParams params = new SetPermissionsParams();
        try {
            params.setId(Long.parseLong(wsNameOrId));
        } catch (NumberFormatException e) {
            params.setWorkspace(wsNameOrId);
        }
        wsClient.setPermissions(params.withUsers(
                Arrays.asList(kbaseIndexerToken.getUserName())).withNewPermission("w"));
    }
    
    public IndexingStorage getIndexingStorage(String objectType) {
        return indexingStorage;
    }
    
    private static boolean toBool(Long value) {
        return value != null && value == 1L;
    }

    private static Integer toInteger(Long value) {
        return value == null ? null : (int)(long)value;
    }

    private static GUID toGUID(String value) {
        return value == null ? null : new GUID(value);
    }

    private kbaserelationengine.search.MatchFilter toSearch(MatchFilter mf) {
        kbaserelationengine.search.MatchFilter ret = 
                new kbaserelationengine.search.MatchFilter()
                .withFullTextInAll(mf.getFullTextInAll())
                .withAccessGroupId(toInteger(mf.getAccessGroupId()))
                .withObjectName(mf.getObjectName())
                .withParentGuid(toGUID(mf.getParentGuid()));
        return ret;
    }

    private kbaserelationengine.search.AccessFilter toSearch(AccessFilter af, String user)
            throws IOException {
        List<Integer> accessGroupIds = accessGroupProvider.findAccessGroupIds("WS", user);
        return new kbaserelationengine.search.AccessFilter()
                .withPublic(toBool(af.getWithPublic()))
                .withAllHistory(toBool(af.getWithAllHistory()))
                .withAccessGroups(new LinkedHashSet<>(accessGroupIds));
    }
    
    public SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception {
        MatchFilter mf = params.getMatchFilter();
        kbaserelationengine.search.MatchFilter matchFilter = toSearch(mf);
        AccessFilter af = params.getAccessFilter();
        kbaserelationengine.search.AccessFilter accessFilter = toSearch(af, user);
        Map<String, Integer> ret = indexingStorage.searchTypes(matchFilter, accessFilter);
        return new SearchTypesOutput().withTypeToCount(ret.keySet().stream().collect(
                Collectors.toMap(Function.identity(), c -> (long)(int)ret.get(c))));
    }
}
