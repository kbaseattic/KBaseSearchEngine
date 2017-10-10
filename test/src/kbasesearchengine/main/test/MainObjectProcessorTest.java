package kbasesearchengine.main.test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import junit.framework.Assert;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.events.storage.OldMongoDBStatusEventStorage;
import kbasesearchengine.events.storage.OldStatusEventStorage;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.search.AccessFilter;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.MatchFilter;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.system.TypeMappingParser;
import kbasesearchengine.system.YAMLTypeMappingParser;
import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.test.controllers.elasticsearch.ElasticSearchController;
import kbasesearchengine.test.controllers.workspace.WorkspaceController;
import kbasesearchengine.test.data.TestDataLoader;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.UObject;
import us.kbase.common.test.TestException;
import us.kbase.common.test.controllers.mongo.MongoController;
import workspace.CreateWorkspaceParams;
import workspace.ObjectSaveData;
import workspace.RegisterTypespecParams;
import workspace.SaveObjectsParams;
import workspace.WorkspaceClient;

public class MainObjectProcessorTest {
	
    private static IndexerCoordinator mop = null;
    private static MongoController mongo;
    private static MongoClient mc;
    private static MongoDatabase db;
    private static ElasticSearchController es;
    private static WorkspaceController ws;
    
    private static int wsid;
    
    private static Path tempDir;
    
    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();
        final URL authURL = TestCommon.getAuthUrl();
        final URL authServiceRootURL;
        if (authURL.toString().contains("api")) {
            authServiceRootURL = new URL(authURL.toString().split("api")[0]);
            System.out.println(String.format("Using %s as auth root URL", authServiceRootURL));
        } else {
            authServiceRootURL = authURL;
        }
        final ConfigurableAuthService authSrv = new ConfigurableAuthService(
                new AuthConfig().withKBaseAuthServerURL(authURL));
        final AuthToken userToken = TestCommon.getToken(authSrv);
        final AuthToken wsadmintoken = TestCommon.getToken2(authSrv);
        if (userToken.getUserName().equals(wsadmintoken.getUserName())) {
            throw new TestException("The test tokens are for the same user");
        }

        tempDir = Paths.get(TestCommon.getTempDir()).resolve("MainObjectProcessorTest");
        // should refactor to just use NIO at some point
        FileUtils.deleteQuietly(tempDir.toFile());
        tempDir.toFile().mkdirs();

        // set up mongo
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                tempDir,
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        final String dbName = "DataStatus";
        db = mc.getDatabase(dbName);
        
        // set up elastic search
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tempDir);
        
        // set up Workspace
        ws = new WorkspaceController(
                TestCommon.getWorkspaceVersion(),
                TestCommon.getJarsDir(),
                "localhost:" + mongo.getServerPort(), "MOPTestWSDB",
                    wsadmintoken.getUserName(),
                authServiceRootURL,
                tempDir);
        System.out.println("Started workspace on port " + ws.getServerPort());
        
        final Path typesDir = Paths.get(TestCommon.TYPES_REPO_DIR);
        final Path mappingsDir = Paths.get(TestCommon.TYPE_MAP_REPO_DIR);
        
        URL wsUrl = new URL("http://localhost:" + ws.getServerPort());

        final String esIndexPrefix = "test_" + System.currentTimeMillis() + ".";
        final HttpHost esHostPort = new HttpHost("localhost", es.getServerPort());
        final LineLogger logger = new LineLogger() {
            @Override
            public void logInfo(String line) {
                System.out.println(line);
            }
            @Override
            public void logError(String line) {
                System.err.println(line);
            }
            @Override
            public void logError(Throwable error) {
                error.printStackTrace();
            }
            @Override
            public void timeStat(GUID guid, long loadMs, long parseMs, long indexMs) {
            }
        };
        final Map<String, TypeMappingParser> parsers = ImmutableMap.of(
                "yaml", new YAMLTypeMappingParser());
        final TypeStorage ss = new TypeFileStorage(typesDir, mappingsDir, parsers, logger);
        
        final OldStatusEventStorage storage = new OldMongoDBStatusEventStorage(db);
        final WorkspaceClient wsClient = new WorkspaceClient(wsUrl, wsadmintoken);
        wsClient.setIsInsecureHttpConnectionAllowed(true); //TODO SEC only do if http
        
        final WorkspaceEventHandler weh = new WorkspaceEventHandler(wsClient);
        // 50k simultaneous users * 1000 group ids each seems like plenty = 50M ints in memory
        
        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort,
                IndexerCoordinator.getTempSubDir(tempDir.toFile(), "esbulk"));
        esStorage.setIndexNamePrefix(esIndexPrefix);
        
        mop = new IndexerCoordinator(Arrays.asList(weh), storage, esStorage,
                ss, tempDir.resolve("MainObjectProcessor").toFile(), logger);
        loadTypes(wsUrl, wsadmintoken);
        wsid = (int) loadTestData(wsUrl, userToken);
    }
    
    private static long loadTestData(final URL wsUrl, final AuthToken usertoken)
            throws IOException, JsonClientException {
        final WorkspaceClient wc = new WorkspaceClient(wsUrl, usertoken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        final long wsid = wc.createWorkspace(new CreateWorkspaceParams().withWorkspace("MOPTest"))
                .getE1();
        
        loadData(wc, wsid, "Narr", "KBaseNarrative.Narrative-1.0", "NarrativeObject1");
        loadData(wc, wsid, "Narr", "KBaseNarrative.Narrative-1.0", "NarrativeObject2");
        loadData(wc, wsid, "Narr", "KBaseNarrative.Narrative-1.0", "NarrativeObject3");
        loadData(wc, wsid, "Narr", "KBaseNarrative.Narrative-1.0", "NarrativeObject4");
        loadData(wc, wsid, "Narr", "KBaseNarrative.Narrative-1.0", "NarrativeObject5");
        
        loadData(wc, wsid, "Assy", "KBaseGenomeAnnotations.Assembly-1.0", "AssemblyObject");
        loadData(wc, wsid, "Genome", "KBaseGenomes.Genome-1.0", "GenomeObject");
        loadData(wc, wsid, "Paired", "KBaseFile.PairedEndLibrary-1.0", "PairedEndLibraryObject");
        loadData(wc, wsid, "reads.2", "KBaseFile.SingleEndLibrary-1.0", "SingleEndLibraryObject");
        return wsid;
    }
    
    private static void loadTypes(final URL wsURL, final AuthToken wsadmintoken) throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(wsURL, wsadmintoken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        loadType(wc, "KBaseFile", "KBaseFile_ci_1477697265343",
                Arrays.asList("SingleEndLibrary", "PairedEndLibrary"));
        loadType(wc, "KBaseGenomeAnnotations", "KBaseGenomeAnnotations_ci_1471308269061",
                Arrays.asList("Assembly"));
        loadType(wc, "KBaseGenomes", "KBaseGenomes_ci_1482357978770", Arrays.asList("Genome"));
        loadType(wc, "KBaseNarrative", "KBaseNarrative_ci_1436483557716",
                Arrays.asList("Narrative"));
    }

    private static void loadData(
            final WorkspaceClient wc,
            final long wsid,
            final String name,
            final String type,
            final String fileName)
            throws JsonParseException, JsonMappingException, IOException, JsonClientException {
        final String data = TestDataLoader.load(fileName);
        final Object objdata = new ObjectMapper().readValue(data, Object.class);
        wc.saveObjects(new SaveObjectsParams()
                .withId(wsid)
                .withObjects(Arrays.asList(new ObjectSaveData()
                        .withData(new UObject(objdata))
                        .withName(name)
                        .withType(type))));
    }

    private static void loadType(
            final WorkspaceClient wc,
            final String module,
            final String fileName,
            final List<String> types)
            throws IOException, JsonClientException {
        final String typespec = TestDataLoader.load(fileName);
        wc.requestModuleOwnership(module);
        final Map<String, String> cmd = new HashMap<>();
        cmd.put("command", "approveModRequest");
        cmd.put("module", module);
        wc.administer(new UObject(cmd));
        wc.registerTypespec(new RegisterTypespecParams()
                .withDryrun(0L)
                .withSpec(typespec)
                .withNewTypes(types));
        wc.releaseModule(module);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        final boolean deleteTempFiles = TestCommon.getDeleteTempFiles();
        if (ws != null) {
            ws.destroy(deleteTempFiles);
        }
        if (mc != null) {
            mc.close();
        }
        if (mongo != null) {
            mongo.destroy(deleteTempFiles);
        }
        if (es != null) {
            es.destroy(deleteTempFiles);
        }
        if (tempDir != null && tempDir.toFile().exists() && deleteTempFiles) {
            FileUtils.deleteQuietly(tempDir.toFile());
        }
    }
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(db);
    }
    
    @Test
    public void testGenomeManually() throws Exception {
        StatusEvent ev = new StatusEvent(
                "-1",
                wsid,
                "3",
                1,
                null,
                null,
                System.currentTimeMillis(),
                new StorageObjectType("WS", "KBaseGenomes.Genome"),
                StatusEventType.NEW_VERSION,
                false);
        mop.processOneEvent(ev);
        PostProcessing pp = new PostProcessing();
        pp.objectInfo = true;
        pp.objectData = true;
        pp.objectKeys = true;
        System.out.println("Genome: " + mop.getIndexingStorage("*").getObjectsByIds(
                mop.getIndexingStorage("*").searchIds("Genome", 
                        MatchFilter.create().withFullTextInAll("test"), null, 
                        AccessFilter.create().withAdmin(true), null).guids, pp).get(0));
        String query = "TrkA";
        Map<String, Integer> typeToCount = mop.getIndexingStorage("*").searchTypes(
                MatchFilter.create().withFullTextInAll(query), 
                AccessFilter.create().withAdmin(true));
        System.out.println("Counts per type: " + typeToCount);
        if (typeToCount.size() == 0) {
            return;
        }
        String type = typeToCount.keySet().iterator().next();
        Set<GUID> guids = mop.getIndexingStorage("*").searchIds(type, 
                MatchFilter.create().withFullTextInAll(query), null, 
                AccessFilter.create().withAdmin(true), null).guids;
        System.out.println("GUIDs found: " + guids);
        ObjectData obj = mop.getIndexingStorage("*").getObjectsByIds(guids, pp).get(0);
        System.out.println("Feature: " + obj);
    }

    private void indexFewVersions(StatusEvent ev) throws Exception {
        for (int i = Math.max(1, ev.getVersion() - 5); i <= ev.getVersion(); i++) {
            mop.processOneEvent(new StatusEvent(ev.getId(),
                    ev.getAccessGroupId(), ev.getAccessGroupObjectId(), i, null,
                    ev.getTargetAccessGroupId(), ev.getTimestamp(), ev.getStorageObjectType(),
                    ev.getEventType(), ev.isGlobalAccessed()));
        }
    }
    
    private void checkSearch(int expectedCount, String type, String query, int accessGroupId,
            boolean debugOutput) throws Exception {
        Set<GUID> ids = mop.getIndexingStorage("*").searchIds(type, 
                MatchFilter.create().withFullTextInAll(query), null, 
                AccessFilter.create().withAccessGroups(accessGroupId), null).guids;
        if (debugOutput) {
            PostProcessing pp = new PostProcessing();
            pp.objectInfo = true;
            pp.objectData = true;
            pp.objectKeys = true;
            System.out.println("DEBUG: " + mop.getIndexingStorage("*").getObjectsByIds(ids, pp));
        }
        Assert.assertEquals(1, ids.size());
    }
    
    @Test
    public void testNarrativeManually() throws Exception {
        indexFewVersions(new StatusEvent(
                "-1",
                wsid,
                "1",
                5,
                null,
                null,
                System.currentTimeMillis(),
                new StorageObjectType("WS", "KBaseNarrative.Narrative"),
                StatusEventType.NEW_VERSION,
                false));
        checkSearch(1, "Narrative", "tree", wsid, false);
        checkSearch(1, "Narrative", "species", wsid, false);
        /*indexFewVersions(new ObjectStatusEvent("-1", "WS", 10455, "1", 78, null, 
                System.currentTimeMillis(), "KBaseNarrative.Narrative", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "Narrative", "Catalog.migrate_module_to_new_git_url", 10455, false);
        checkSearch(1, "Narrative", "Super password!", 10455, false);
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 480, "1", 254, null, 
                System.currentTimeMillis(), "KBaseNarrative.Narrative", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "Narrative", "weird text", 480, false);
        checkSearch(1, "Narrative", "functionality", 480, false);*/
    }
    
    @Test
    public void testReadsManually() throws Exception {
        indexFewVersions(new StatusEvent(
                "-1",
                wsid,
                "4",
                1,
                null,
                null,
                System.currentTimeMillis(),
                new StorageObjectType("WS", "KBaseFile.PairedEndLibrary"),
                StatusEventType.NEW_VERSION,
                false));
        checkSearch(1, "PairedEndLibrary", "Illumina", wsid, true);
        checkSearch(1, "PairedEndLibrary", "sample1se.fastq.gz", wsid, false);
        indexFewVersions(new StatusEvent(
                "-1",
                wsid,
                "5",
                1,
                null,
                null,
                System.currentTimeMillis(),
                new StorageObjectType("WS", "KBaseFile.SingleEndLibrary"),
                StatusEventType.NEW_VERSION,
                false));
        checkSearch(1, "SingleEndLibrary", "PacBio", wsid, true);
        checkSearch(1, "SingleEndLibrary", "reads.2", wsid, false);
    }
}
