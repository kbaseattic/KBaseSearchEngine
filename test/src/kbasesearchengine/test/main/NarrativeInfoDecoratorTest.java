package kbasesearchengine.test.main;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.authorization.AccessGroupProvider;
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
import kbasesearchengine.main.DecorateWithNarrativeInfo;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.SearchMethods;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.UObject;
import us.kbase.common.test.controllers.mongo.MongoController;
import us.kbase.test.auth2.authcontroller.AuthController;
import us.kbase.workspace.CreateWorkspaceParams;
import us.kbase.workspace.ObjectSaveData;
import us.kbase.workspace.RegisterTypespecParams;
import us.kbase.workspace.SaveObjectsParams;
import us.kbase.workspace.WorkspaceClient;

public class NarrativeInfoDecoratorTest {

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
    private static WorkspaceEventHandler weh;
    private static MongoDatabase wsdb;
    private static WorkspaceClient wsCli1, wsClient;
    private static AuthToken userToken;

    private static Path tempDirPath;

    private static SearchInterface search;

    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();

        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("SearchMethodsDecoratorTest");
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
        wsClient = new WorkspaceClient(wsUrl, wsadmintoken);
        wsClient.setIsInsecureHttpConnectionAllowed(true);

        //final WorkspaceEventHandler weh = new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(wsClient));

        weh = new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(wsClient));

        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort,
                FileUtil.getOrCreateSubDir(tempDirPath.toFile(), "esbulk"));
        esStorage.setIndexNamePrefix(esIndexPrefix);
        indexStorage = esStorage;

        List intList = new ArrayList<Integer>(Arrays.asList(1,2,4,5));

        final AccessGroupProvider accessGroupProvider = mock(AccessGroupProvider.class);
        when(accessGroupProvider.findAccessGroupIds("user1")).
                thenReturn(intList);

        System.out.println("Creating Search Methods");
        search = new SearchMethods(accessGroupProvider, indexStorage, ss, Collections.emptySet());

        System.out.println("Creating indexer worker");
        File tempDir = tempDirPath.resolve("WorkerTemp").toFile();

        tempDir.mkdirs();
        worker = new IndexerWorker("test", Arrays.asList(weh), storage, indexStorage,
                ss, tempDir, logger, null);
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
        ownModule(wc, "TwoVersions");
        ownModule(wc, "TwoVersionsMapped");
        ownModule(wc, "NoIndexingRules");
        loadType(wc, "Empty", "Empty.spec", Arrays.asList("AType"));
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

    private static String loadData(
            final WorkspaceClient wc,
            final long wsid,
            final String name,
            final String type,
            final String data1,
            final String data2)
            throws JsonParseException, JsonMappingException, IOException, JsonClientException {

        List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>> retVal =
                wc.saveObjects(new SaveObjectsParams()
                        .withId(wsid)
                        .withObjects(Arrays.asList(new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(data1, data2)))
                                .withName(name)
                                .withType(type))));
        return retVal.get(0).getE7().toString() + '/' +
                retVal.get(0).getE1().toString() +'/' +
                retVal.get(0).getE5().toString();
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
    public void testObjectDecorator() throws Exception {
        // a basic test to ensure all the indexer guts are working together.
        // also tests provenance

        final long wsId1 = wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("workspace-1")).getE1();
        final long wsId2 = wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("workspace-2")).getE1();

        String emptyRef1 = loadData(wsCli1, wsId1, "EmptyObj1", "Empty.AType-1.0", "EmptyObject-1", "Obj-1-data");
        String emptyRef2 = loadData(wsCli1, wsId1, "EmptyObj2", "Empty.AType-1.0", "EmptyObject-2", "Obj-2-data");
        String emptyRef3 = loadData(wsCli1, wsId2, "EmptyObj3", "Empty.AType-1.0", "EmptyObject-3", "Obj-3-data");

        System.out.println(" EMPTY REF1: " + emptyRef1);
        System.out.println(" EMPTY REF2: " + emptyRef2);
        System.out.println(" EMPTY REF3: " + emptyRef3);

        System.out.println("waiting 5s for event to trickle through the system");
        Thread.sleep(5000); // wait for the indexer & worker to process the event

        List<String> guid_list = new ArrayList<String>();
        guid_list.add("WS:" + emptyRef1);
        guid_list.add("WS:" + emptyRef2);
        guid_list.add("WS:" + emptyRef3);

        System.out.println(" IN TEST DECORATOR");

        // test getObjects

        GetObjectsInput objsInput = new GetObjectsInput().withGuids(guid_list);
        System.out.println(" GET OBJS INPUT: " + objsInput.toString());

        GetObjectsOutput objsOutput = search.getObjects(objsInput, userToken.getUserName());
        System.out.println(" GET OBJS OUTPUT: " + objsOutput.toString());

        SearchInterface sid = new DecorateWithNarrativeInfo(search,
                new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(wsCli1)));
        GetObjectsOutput objsOutputDecorated = sid.getObjects(objsInput, userToken.getUserName());
        System.out.println(" GET OBJS OUTPUT WITH NARRATIVE INFO: " + objsOutputDecorated.toString());

        String guid = objsOutputDecorated.getObjects().get(0).getGuid();
        String objName = objsOutputDecorated.getObjects().get(0).getObjectName();
        Map<Long, Tuple5 <String, Long, Long, String, String>> narrtiveInfo =
                objsOutputDecorated.getAccessGroupNarrativeInfo();
        
        assertNotNull("incorrect narrative info", narrtiveInfo);
        assertThat("incorrect user name", narrtiveInfo.get(new Long(1)).getE4(), is("user1"));
        assertThat("incorrect user name", narrtiveInfo.get(new Long(2)).getE4(), is("user1"));
    }

}
