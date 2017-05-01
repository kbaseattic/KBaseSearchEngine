package kbaserelationengine.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.WSStatusEventTrigger;
import kbaserelationengine.events.reconstructor.AccessType;
import kbaserelationengine.events.reconstructor.PresenceType;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructor;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbaserelationengine.events.storage.FakeStatusStorage;
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
    private ObjectStatusEventQueue queue;
    private Thread mainRunner;
    private SystemStorage systemStorage;
    private IndexingStorage indexingStorage;
    private RelationStorage relationStorage;
    
    public MainObjectProcessor(URL wsURL, AuthToken kbaseIndexerToken, String mongoHost, 
            int mongoPort, String mongoDbName, HttpHost esHost, String esUser, String esPassword,
            String esIndexPrefix, File typesDir, File tempDir, boolean startLifecycleRunner) 
                    throws IOException, ObjectParseException {
        this.wsURL = wsURL;
        this.kbaseIndexerToken = kbaseIndexerToken;
        this.rootTempDir = tempDir;
        eventStorage = new FakeStatusStorage(); //new MongoDBStatusEventStorage(mongoHost, mongoPort, mongoDbName);
        WSStatusEventTrigger eventTrigger = new WSStatusEventTrigger();
        wsEventReconstructor = new WSStatusEventReconstructorImpl(wsURL, kbaseIndexerToken, 
                eventStorage, eventTrigger);
        //eventTrigger.registerListener((StatusEventListener)eventStorage);
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
                            String codePlace = e.getStackTrace().length > 0 ? 
                                    e.getStackTrace()[0].toString() : "<not-available>";
                                    System.out.println("Error in Lifecycle runner: " + e + ", " + codePlace);
                        }
                    }
                } finally {
                    mainRunner = null;
                }
            }
        });
        mainRunner.start();
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
        wsEventReconstructor.processWorkspaceObjects(AccessType.PRIVATE, PresenceType.PRESENT, 
                Collections.emptySet());
        ObjectStatusEventIterator iter = queue.iterator("WS");
        while (iter.hasNext()) {
            ObjectStatusEvent ev = iter.next();
            if (!isStorageTypeSupported(ev.getStorageObjectType())) {
                System.out.println("Skipping " + ev.getEventType() + ", " + 
                        ev.getStorageObjectType() + ", " + ev.toGUID());
                iter.markAsVisitied(false);
                continue;
            }
            System.out.println("Processing " + ev.getEventType() + ", " + 
                    ev.getStorageObjectType() + ", " + ev.toGUID() + "...");
            long time = System.currentTimeMillis();
            processOneEvent(ev);
            iter.markAsVisitied(true);
            System.out.println("    (processing time: " + (System.currentTimeMillis() - time) + "ms.)");
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
        System.out.println("Processing object: " + guid);
        long t1 = System.currentTimeMillis();
        File tempFile = ObjectParser.prepareTempFile(getWsLoadTempDir());
        try {
            String objRef = guid.getAccessGroupId() + "/" + guid.getAccessGroupObjectId() + "/" +
                    guid.getVersion();
            ObjectData obj = ObjectParser.loadObject(wsURL, tempFile, kbaseIndexerToken, objRef);
            System.out.println("  Loading time: " + (System.currentTimeMillis() - t1) + " ms.");
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
                System.out.println("  " + rule.getGlobalObjectType() + ": parsing time: " + 
                        (System.currentTimeMillis() - t2) + " ms.");
                long t3 = System.currentTimeMillis();
                if (timestamp == null) {
                    timestamp = System.currentTimeMillis();
                }
                indexingStorage.indexObjects(rule.getGlobalObjectType(), obj.getInfo().getE2(), 
                        timestamp, parentJson, obj.getInfo().getE11(), guidToJson, 
                        false, rule.getIndexingRules());
                System.out.println("  " + rule.getGlobalObjectType() + ": indexing time: " + 
                        (System.currentTimeMillis() - t3) + " ms.");
            }
        } finally {
            tempFile.delete();
        }
    }
    
    public void share(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.shareObject(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
    }

    public void unshare(GUID guid, int accessGroupId) throws IOException {
        indexingStorage.unshareObject(new LinkedHashSet<>(Arrays.asList(guid)), accessGroupId);
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
}
