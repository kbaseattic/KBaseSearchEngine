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
import java.util.Collections;
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
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.UObject;
import us.kbase.common.test.controllers.mongo.MongoController;
import us.kbase.test.auth2.authcontroller.AuthController;
import us.kbase.workspace.CreateWorkspaceParams;
import us.kbase.workspace.GetObjects2Params;
import us.kbase.workspace.ObjectSaveData;
import us.kbase.workspace.ObjectSpecification;
import us.kbase.workspace.ProvenanceAction;
import us.kbase.workspace.RegisterTypespecParams;
import us.kbase.workspace.SaveObjectsParams;
import us.kbase.workspace.SubAction;
import us.kbase.workspace.WorkspaceClient;

public class IndexerIntegrationTest {
    
    // should add a setting in the worker and coordinator to shorten the wait time for testing
    // purposes
    
    private static AuthController auth = null;
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

        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("IndexerIntegrationTest");
        // should refactor to just use NIO at some point
        FileUtils.deleteQuietly(tempDirPath.toFile());
        tempDirPath.toFile().mkdirs();
        final Path searchTypesDir = Files.createDirectories(tempDirPath.resolve("searchtypes"));
        installSearchTypes(searchTypesDir);
        final Path mappingsDir = Files.createDirectories(tempDirPath.resolve("searchmappings"));
        installSearchMappings(mappingsDir);

        // set up mongo
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                tempDirPath,
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        final String eventDBName = "DataStatus";
        db = mc.getDatabase(eventDBName);
        
        // set up auth
        auth = new AuthController(
                TestCommon.getJarsDir(),
                "localhost:" + mongo.getServerPort(),
                "IndexerIntTestAuth",
                tempDirPath);
        final URL authURL = new URL("http://localhost:" + auth.getServerPort() + "/testmode");
        System.out.println("started auth server at " + authURL);
        TestCommon.createAuthUser(authURL, "user1", "display1");
        TestCommon.createAuthUser(authURL, "user2", "display2");
        final String token1 = TestCommon.createLoginToken(authURL, "user1");
        final String token2 = TestCommon.createLoginToken(authURL, "user2");
        userToken = new AuthToken(token1, "user1");
        final AuthToken wsadmintoken = new AuthToken(token2, "user2");
        
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
                authURL,
                tempDirPath);
        System.out.println("Started workspace on port " + ws.getServerPort());
        wsdb = mc.getDatabase("IndexerIntegTestWSDB");
        
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
        File tempDir = tempDirPath.resolve("WorkerTemp").toFile();
        tempDir.mkdirs();
        worker = new IndexerWorker("test", Arrays.asList(weh), storage, indexStorage,
                ss, tempDir, logger, null, 1000);
        System.out.println("Starting indexer worker");
        worker.startIndexer();
        System.out.println("Creating indexer coordinator");
        coord = new IndexerCoordinator(storage, logger, 10);
        System.out.println("Starting indexer coordinator");
        coord.startIndexer();
        loadWSTypes(wsUrl, wsadmintoken);
    }
    
    private static void installSearchTypes(final Path target) throws IOException {
        installTestFile("EmptyAType.json", target);
        installTestFile("OneStringThreeKeyNames.yaml", target);
        installTestFile("TwoVersions.yaml", target);
        installTestFile("NoIndexingRules.yaml", target);
    }
    
    private static void installTestFile(final String fileName, final Path target)
            throws IOException {
        final String file = TestDataLoader.load(fileName);
        Files.write(target.resolve(fileName), file.getBytes());
    }
    
    private static void installSearchMappings(final Path target) throws IOException {
        installTestFile("TwoVersionsMapping.yaml", target);
    }
    
    private static void loadWSTypes(final URL wsURL, final AuthToken wsadmintoken)
            throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(wsURL, wsadmintoken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        ownModule(wc, "Empty");
        ownModule(wc, "OneString");
        ownModule(wc, "TwoVersions");
        ownModule(wc, "TwoVersionsMapped");
        ownModule(wc, "NoIndexingRules");
        loadType(wc, "Empty", "Empty.spec", Arrays.asList("AType"));
        loadType(wc, "OneString", "OneString.spec", Arrays.asList("AType"));
        loadType(wc, "TwoVersions", "TwoVersions1.spec", Arrays.asList("Type"));
        loadType(wc, "TwoVersions", "TwoVersions2.spec", Collections.emptyList());
        loadType(wc, "TwoVersionsMapped", "TwoVersionsMapped1.spec", Arrays.asList("Type"));
        loadType(wc, "TwoVersionsMapped", "TwoVersionsMapped2.spec", Collections.emptyList());
        loadType(wc, "NoIndexingRules", "NoIndexingRules.spec", Arrays.asList("Type"));
    }
    
    private static void ownModule(final WorkspaceClient wc, final String module)
            throws IOException, JsonClientException {
        wc.requestModuleOwnership(module);
        final Map<String, String> cmd = new HashMap<>();
        cmd.put("command", "approveModRequest");
        cmd.put("module", module);
        wc.administer(new UObject(cmd));
    }

    private static void loadType(
            final WorkspaceClient wc,
            final String module,
            final String fileName,
            final List<String> types)
            throws IOException, JsonClientException {
        final String typespec = TestDataLoader.load(fileName);
        System.out.println(String.format("Loading type %s to workspace", module));
        wc.registerTypespec(new RegisterTypespecParams()
                .withDryrun(0L)
                .withSpec(typespec)
                .withNewTypes(types));
        System.out.println("released: " + wc.releaseModule(module));
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
        if (auth != null) {
            auth.destroy(deleteTempFiles);
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
        indexStorage.dropData();
    }

    @Test
    public void singleNewVersionWithSourceTags() throws Exception {
        // a basic test to ensure all the indexer guts are working together.
        // also tests provenance and source tags
        
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo")
                .withMeta(ImmutableMap.of("searchtags", "narrative, refdata")));
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
        final long timestamp = getWSTimeStamp("1/1/1");
        
        System.out.println("waiting 5s for event to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        final Instant indexedTimestamp = indexedObj.getTimestamp().get();
        
        final ObjectData expected = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("bar")
                .withNullableType(new SearchObjectType("EmptyAType", 1))
                .withNullableCreator(userToken.getUserName())
                .withNullableModule("serv")
                .withNullableMethod("meth")
                .withNullableCommitHash("commit")
                .withNullableModuleVersion("servver")
                .withNullableMD5("3c6e8d4dde8a26a0bfca203228cc6a36")
                .withNullableTimestamp(indexedTimestamp)
                .withNullableData(ImmutableMap.of("whee", "wugga"))
                .withKeyProperty("whee", "wugga")
                .withSourceTag("narrative")
                .withSourceTag("refdata")
                .build();
        
        assertThat("incorrect indexed object", indexedObj, is(expected));
        assertWSTimestampCloseToIndexedTimestamp(timestamp, indexedTimestamp);
    }
    
    @Test
    public void threeKeyNames() throws Exception {
        // tests that a spec with multiple keynames for the same field works.
        
        wsCli1.createWorkspace(new CreateWorkspaceParams().withWorkspace("foo"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo")
                .withObjects(Arrays.asList(new ObjectSaveData()
                        .withData(new UObject(ImmutableMap.of("foo", "bar")))
                        .withName("bar")
                        .withType("OneString.AType-1.0")
                ))
        );
        final long timestamp = getWSTimeStamp("1/1/1");
        
        System.out.println("waiting 5s for event to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        final Instant indexedTimestamp = indexedObj.getTimestamp().get();
        
        final ObjectData expected = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("bar")
                .withNullableType(new SearchObjectType("OneString", 1))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("9bb58f26192e4ba00f01e2e7b136bbd8")
                .withNullableTimestamp(indexedTimestamp)
                .withNullableData(ImmutableMap.of("foo", "bar"))
                .withKeyProperty("foo", "bar")
                .withKeyProperty("foo1", "bar")
                .withKeyProperty("foo2", "bar")
                .build();
        
        assertThat("incorrect indexed object", indexedObj, is(expected));
        assertWSTimestampCloseToIndexedTimestamp(timestamp, indexedTimestamp);
    }

    private void assertWSTimestampCloseToIndexedTimestamp(
            final long workspaceTimestamp,
            final Instant indexedTimestamp) {
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
                Instant.ofEpochMilli(workspaceTimestamp), indexedTimestamp, 0, 100);
    }
    
    @Test
    public void twoVersionsWithoutMapping() throws Exception {
        // should always use the 2nd version of the spec for any ws type version
        // since there are no type mappings
        
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "one")))
                                .withName("obj1")
                                .withType("TwoVersions.Type-1.0"),
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "whug",
                                        "whoo", "gofasterstripes",
                                        "req", 1)))
                                .withName("obj2")
                                .withType("TwoVersions.Type-2.0")
                ))
        );
        
        final long timestamp1 = getWSTimeStamp("1/1/1");
        final long timestamp2 = getWSTimeStamp("1/2/1");
        
        System.out.println("waiting 5s for events to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj1 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        final Instant indexedTimestamp1 = indexedObj1.getTimestamp().get();
        
        final ObjectData indexedObj2 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/2/1"))).get(0);
        final Instant indexedTimestamp2 = indexedObj2.getTimestamp().get();
        
        final ObjectData expected1 = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("obj1")
                .withNullableType(new SearchObjectType("TwoVers", 2))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("d20dd9b7a7cd69471b2b13ae7593de90")
                .withNullableTimestamp(indexedTimestamp1)
                .withNullableData(ImmutableMap.of("whee", "wugga", "whoo", "thingy"))
                .withKeyProperty("whee", "wugga")
                .withKeyProperty("whoo", "thingy")
                .build();
        
        final ObjectData expected2 = ObjectData.getBuilder(new GUID("WS:1/2/1"))
                .withNullableObjectName("obj2")
                .withNullableType(new SearchObjectType("TwoVers", 2))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("51368afbd22bcf7987b98ca28607c67d")
                .withNullableTimestamp(indexedTimestamp2)
                .withNullableData(ImmutableMap.of("whee", "whug", "whoo", "gofasterstripes"))
                .withKeyProperty("whee", "whug")
                .withKeyProperty("whoo", "gofasterstripes")
                .build();
        
        assertThat("incorrect indexed object", indexedObj1, is(expected1));
        assertWSTimestampCloseToIndexedTimestamp(timestamp1, indexedTimestamp1);
        
        assertThat("incorrect indexed object", indexedObj2, is(expected2));
        assertWSTimestampCloseToIndexedTimestamp(timestamp2, indexedTimestamp2);
    }
    
    @Test
    public void twoVersionsWithMapping() throws Exception {
        
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "one")))
                                .withName("obj1")
                                .withType("TwoVersionsMapped.Type-1.0"),
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "whug",
                                        "whoo", "gofasterstripes",
                                        "req", 1)))
                                .withName("obj2")
                                .withType("TwoVersionsMapped.Type-2.0")
                ))
        );
        
        final long timestamp1 = getWSTimeStamp("1/1/1");
        final long timestamp2 = getWSTimeStamp("1/2/1");
        
        System.out.println("waiting 5s for events to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj1 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        final Instant indexedTimestamp1 = indexedObj1.getTimestamp().get();
        
        final ObjectData indexedObj2 =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/2/1"))).get(0);
        final Instant indexedTimestamp2 = indexedObj2.getTimestamp().get();
        
        final ObjectData expected1 = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("obj1")
                .withNullableType(new SearchObjectType("TwoVers", 1))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("d20dd9b7a7cd69471b2b13ae7593de90")
                .withNullableTimestamp(indexedTimestamp1)
                .withNullableData(ImmutableMap.of("whee", "wugga"))
                .withKeyProperty("whee", "wugga")
                .build();
        
        final ObjectData expected2 = ObjectData.getBuilder(new GUID("WS:1/2/1"))
                .withNullableObjectName("obj2")
                .withNullableType(new SearchObjectType("TwoVers", 2))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("51368afbd22bcf7987b98ca28607c67d")
                .withNullableTimestamp(indexedTimestamp2)
                .withNullableData(ImmutableMap.of("whee", "whug", "whoo", "gofasterstripes"))
                .withKeyProperty("whee", "whug")
                .withKeyProperty("whoo", "gofasterstripes")
                .build();
        
        assertThat("incorrect indexed object", indexedObj1, is(expected1));
        assertWSTimestampCloseToIndexedTimestamp(timestamp1, indexedTimestamp1);
        
        assertThat("incorrect indexed object", indexedObj2, is(expected2));
        assertWSTimestampCloseToIndexedTimestamp(timestamp2, indexedTimestamp2);
    }
    
    @Test
    public void noIndexingRules() throws Exception {
        // tests that a search spec without any indexing rules still indexes the general object
        // properties
        
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "one")))
                                .withName("obj1")
                                .withType("NoIndexingRules.Type-1.0")
                ))
        );
        
        final long timestamp = getWSTimeStamp("1/1/1");
        
        System.out.println("waiting 5s for events to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event
        
        final ObjectData indexedObj =
                indexStorage.getObjectsByIds(TestCommon.set(new GUID("WS:1/1/1"))).get(0);
        final Instant indexedTimestamp = indexedObj.getTimestamp().get();
        
        final ObjectData expected = ObjectData.getBuilder(new GUID("WS:1/1/1"))
                .withNullableObjectName("obj1")
                .withNullableType(new SearchObjectType("NoIndexRules", 1))
                .withNullableCreator(userToken.getUserName())
                .withNullableMD5("d20dd9b7a7cd69471b2b13ae7593de90")
                .withNullableTimestamp(indexedTimestamp)
                .build();
        
        assertThat("incorrect indexed object", indexedObj, is(expected));
        assertWSTimestampCloseToIndexedTimestamp(timestamp, indexedTimestamp);
    }

    private long getWSTimeStamp(final String ref) throws IOException, JsonClientException {
        return wsCli1.getObjects2(new GetObjects2Params()
                .withNoData(1L)
                .withObjects(Arrays.asList(new ObjectSpecification().withRef(ref))))
                .getData().get(0).getEpoch();
    }
    
}
