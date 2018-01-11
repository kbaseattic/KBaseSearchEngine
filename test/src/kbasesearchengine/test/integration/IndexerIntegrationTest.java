package kbasesearchengine.test.integration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kbasesearchengine.common.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.TypeMappingParser;
import kbasesearchengine.system.TypeStorage;
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
import workspace.GetObjects2Params;
import workspace.ObjectSaveData;
import workspace.ObjectSpecification;
import workspace.ProvenanceAction;
import workspace.RegisterTypespecParams;
import workspace.SaveObjectsParams;
import workspace.SubAction;
import workspace.WorkspaceClient;

public class IndexerIntegrationTest {
    
    private static IndexerWorker worker = null;
    private static IndexerCoordinator coord = null;
    private static MongoController mongo;
    private static MongoClient mc;
    private static MongoDatabase db;
    private static ElasticSearchController es;
    private static IndexingStorage indexStorage;
    private static WorkspaceController ws;
    private static MongoDatabase wsdb;
    private static WorkspaceClient wsCli1;
    private static AuthToken userToken;
    
    private static Path tempDirPath;
    
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
        userToken = TestCommon.getToken(authSrv);
        final AuthToken wsadmintoken = TestCommon.getToken2(authSrv);
        if (userToken.getUserName().equals(wsadmintoken.getUserName())) {
            throw new TestException("The test tokens are for the same user");
        }

        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("MainObjectProcessorTest");
        // should refactor to just use NIO at some point
        FileUtils.deleteQuietly(tempDirPath.toFile());
        tempDirPath.toFile().mkdirs();
        final Path searchTypesDir = Files.createDirectories(tempDirPath.resolve("searchtypes"));
        installSearchTypes(searchTypesDir);

        // set up mongo
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                tempDirPath,
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        final String eventDBName = "DataStatus";
        db = mc.getDatabase(eventDBName);
        
        // set up elastic search
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tempDirPath);
        
        // set up Workspace
        ws = new WorkspaceController(
                TestCommon.getWorkspaceVersion(),
                TestCommon.getJarsDir(),
                "localhost:" + mongo.getServerPort(),
                "IndexerIntegTestWSDB",
                eventDBName,
                wsadmintoken.getUserName(),
                authServiceRootURL,
                tempDirPath);
        System.out.println("Started workspace on port " + ws.getServerPort());
        wsdb = mc.getDatabase("IndexerIntegTestWSDB");
        
        final Path mappingsDir = Paths.get(TestCommon.TYPE_MAP_REPO_DIR);
        
        URL wsUrl = new URL("http://localhost:" + ws.getServerPort());
        wsCli1 = new WorkspaceClient(wsUrl, userToken);
        wsCli1.setIsInsecureHttpConnectionAllowed(true);

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
        final TypeStorage ss = new TypeFileStorage(searchTypesDir, mappingsDir,
                new ObjectTypeParsingRulesFileParser(), parsers, new FileLister(), logger);
        
        final StatusEventStorage storage = new MongoDBStatusEventStorage(db);
        final WorkspaceClient wsClient = new WorkspaceClient(wsUrl, wsadmintoken);
        wsClient.setIsInsecureHttpConnectionAllowed(true);
        
        final WorkspaceEventHandler weh = new WorkspaceEventHandler(
                new CloneableWorkspaceClientImpl(wsClient));
        
        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort,
                FileUtil.getOrCreateSubDir(tempDirPath.toFile(), "esbulk"));
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexStorage = esStorage;
        
        System.out.println("Creating indexer worker");
        File tempDir = tempDirPath.resolve("MainObjectProcessor").toFile();
        tempDir.mkdirs();
        worker = new IndexerWorker("test", Arrays.asList(weh), storage, indexStorage,
                ss, tempDir, logger);
        System.out.println("Starting indexer worker");
        worker.startIndexer();
        System.out.println("Creating indexer coordinator");
        coord = new IndexerCoordinator(storage, logger, 10);
        System.out.println("Starting indexer coordinator");
        coord.startIndexer();
        loadWSTypes(wsUrl, wsadmintoken);
    }
    
    private static void installSearchTypes(final Path target) throws IOException {
        installSearchType("EmptyAType.json", target);
    }
    
    private static void installSearchType(final String fileName, final Path target)
            throws IOException {
        final String file = TestDataLoader.load(fileName);
        Files.write(target.resolve(fileName), file.getBytes());
    }
    
    private static void loadWSTypes(final URL wsURL, final AuthToken wsadmintoken)
            throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(wsURL, wsadmintoken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        loadType(wc, "Empty", "Empty.spec", Arrays.asList("AType"));
    }

    private static void loadType(
            final WorkspaceClient wc,
            final String module,
            final String fileName,
            final List<String> types)
            throws IOException, JsonClientException {
        final String typespec = TestDataLoader.load(fileName);
        System.out.println(String.format("Loading type %s to workspace", module));
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
        if (coord != null) {
            coord.stop(0);
        }
        if (worker != null) {
            worker.stop(0);
        }
        
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
        if (tempDirPath != null && tempDirPath.toFile().exists() && deleteTempFiles) {
            FileUtils.deleteQuietly(tempDirPath.toFile());
        }
    }
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(db);
        TestCommon.destroyDB(wsdb);
        // should drop elastic indexes as well
    }

    @Test
    public void singleNewVersion() throws Exception {
        // a basic test to ensure all the indexer guts are working together.
        
        // should add a setting in the worker and coordinator to shorten the wait time for testing
        // purposes
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo")
                .withObjects(Arrays.asList(new ObjectSaveData()
                        .withData(new UObject(ImmutableMap.of("whee", "wugga")))
                        .withName("bar")
                        .withType("Empty.AType-1.0")
                        .withProvenance(Arrays.asList(new ProvenanceAction()
                                .withService("serv")
                                .withMethod("meth")
                                .withServiceVer("servver")
                                .withSubactions(Arrays.asList(new SubAction()
                                        .withCommit("commit")
                                        .withName("serv.meth")
                                ))
                        ))
                ))
        );
        final long timestamp = wsCli1.getObjects2(new GetObjects2Params()
                .withNoData(1L)
                .withObjects(Arrays.asList(new ObjectSpecification()
                        .withRef("1/1/1")))).getData().get(0).getEpoch();
        
        System.out.println("waiting 5s for event to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        
        final ObjectData expected = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("bar")
                .withNullableType(new SearchObjectType("EmptyAType", 1))
                .withNullableCreator(userToken.getUserName())
                .withNullableModule("serv")
                .withNullableMethod("meth")
                .withNullableCommitHash("commit")
                .withNullableModuleVersion("servver")
                .withNullableMD5("3c6e8d4dde8a26a0bfca203228cc6a36")
                .withNullableTimestamp(indexedObj.getTimestamp().get())
                .withNullableData(ImmutableMap.of("whee", "wugga"))
                .withKeyProperty("whee", "wugga")
                .build();
        
        assertThat("incorrect indexed object", indexedObj, is(expected));
        
        /* it turns out the ws provenance timestamp and obj_info timestamps are not identical
         * for a freshly saved object since the provenance is saved first. Furthermore, obj_info
         * timestamps have no millisecond info for backwards compatibility reasons
         * (should just return longs rather than strings in a UI revamp).
         * Hence, we just check if the provenance timestamp, for which we do have millisecond
         * information, is close the the elasticsearch timestamp, which comes from the timestamp
         * that's used to create the obj_info string.
         * 
         * This timestamp is passed to the search service via the NEW_VERSION event, and so
         * has millisecond info.
         */
        
        TestCommon.assertCloseMS(
                Instant.ofEpochMilli(timestamp), indexedObj.getTimestamp().get(), 0, 100);
    }
    
}
