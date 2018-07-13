package kbasesearchengine.test.integration;

import static kbasesearchengine.test.common.TestCommon.set;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.objTuple;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareWsInfo;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfo;
import static kbasesearchengine.test.main.NarrativeInfoDecoratorTest.narrInfoTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
import java.util.Set;
import java.util.HashSet;

import java.util.stream.Collectors;

import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.AccessFilter;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.KBaseSearchEngineClient;
import kbasesearchengine.KBaseSearchEngineServer;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.PostProcessing;
import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SortingRule;
import kbasesearchengine.authorization.TemporaryAuth2Client;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;
import kbasesearchengine.common.FileUtil;
import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.test.controllers.elasticsearch.ElasticSearchController;
import kbasesearchengine.test.controllers.workspace.WorkspaceController;
import kbasesearchengine.test.data.TestDataLoader;
import kbasesearchengine.test.main.NarrativeInfoDecoratorTest;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.ServerException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.UObject;
import us.kbase.common.test.controllers.mongo.MongoController;
import us.kbase.test.auth2.authcontroller.AuthController;
import us.kbase.workspace.CreateWorkspaceParams;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.RegisterTypespecParams;

import us.kbase.workspace.SaveObjectsParams;
import us.kbase.common.service.JsonClientException;
import us.kbase.workspace.ObjectSaveData;


public class SearchAPIIntegrationTest {

    /* Tests the integration of the compiled search server, translation classes, and index storage
     * classes.
     * 
     * As of 2/6/2018, these are:
     * KBaseSearchEngineServer
     * SearchMethods
     * ElasticIndexingStorage
     * 
     * ... and associated classes. Decorators for SearchMethods were in the works.
     * 
     */
    
    //TODO TEST add more tests. Should have reasonable integration tests for basic happy paths and a few unhappy paths.
    
    private static Path tempDirPath;
    private static Path searchTypesDir;
    private static Path mappingsDir;
    private static String esIndexPrefix;
    private static MongoController mongo;
    private static AuthController auth;
    private static AuthToken userToken;
    private static AuthToken wsadmintoken;
    private static ElasticSearchController es;
    private static ElasticIndexingStorage indexStorage;
    private static WorkspaceController ws;
    private static MongoDatabase wsdb;
    private static MongoClient mc;
    private static WorkspaceClient wsCli1;
    private static KBaseSearchEngineServer searchServer;
    private static KBaseSearchEngineClient searchCli;
    private static KBaseSearchEngineClient noAuthSearchCli;
    private static URL authURL;
    private static int wsid;
    private static WorkspaceEventHandler weh;

    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();

        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("SearchAPIIntegrationTest");
        // should refactor to just use NIO at some point
        FileUtils.deleteQuietly(tempDirPath.toFile());
        tempDirPath.toFile().mkdirs();
        searchTypesDir = Files.createDirectories(tempDirPath.resolve("searchtypes"));
        installSearchTypes(searchTypesDir);
        mappingsDir = Files.createDirectories(tempDirPath.resolve("searchmappings"));
        installSearchMappings(mappingsDir);

        // set up mongo, needed for auth and workspace
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                tempDirPath,
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());

        // set up auth
        auth = new AuthController(
                TestCommon.getJarsDir(),
                "localhost:" + mongo.getServerPort(),
                "SearchAPIIntTestAuth",
                tempDirPath);
        authURL = new URL("http://localhost:" + auth.getServerPort() + "/testmode");
        System.out.println("started auth server at " + authURL);
        TestCommon.createAuthUser(authURL, "user1", "display1");
        TestCommon.createAuthUser(authURL, "user2", "display2");
        final String token1 = TestCommon.createLoginToken(authURL, "user1");
        final String token2 = TestCommon.createLoginToken(authURL, "user2");
        userToken = new AuthToken(token1, "user1");
        wsadmintoken = new AuthToken(token2, "user2");

        // set up elastic search
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tempDirPath);
        
        // set up Workspace, only needed for getting allowed access group ids
        ws = new WorkspaceController(
                TestCommon.getWorkspaceVersion(),
                TestCommon.getJarsDir(),
                "localhost:" + mongo.getServerPort(),
                "SearchAPIIntTestWSDB",
                "TestEventDB",
                wsadmintoken.getUserName(),
                authURL,
                tempDirPath);
        System.out.println("Started workspace on port " + ws.getServerPort());
        wsdb = mc.getDatabase("SearchAPIIntTestWSDB");
        
        URL wsUrl = new URL("http://localhost:" + ws.getServerPort());
        wsCli1 = new WorkspaceClient(wsUrl, userToken);
        wsCli1.setIsInsecureHttpConnectionAllowed(true);

        final WorkspaceClient wsClient = new WorkspaceClient(wsUrl, wsadmintoken);
        wsClient.setIsInsecureHttpConnectionAllowed(true);

        weh = new WorkspaceEventHandler(
                new CloneableWorkspaceClientImpl(wsClient));

        esIndexPrefix = "test_" + System.currentTimeMillis();
        final HttpHost esHostPort = new HttpHost("localhost", es.getServerPort());

        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort,
                FileUtil.getOrCreateSubDir(tempDirPath.toFile(), "esbulk"));
        esStorage.setIndexNamePrefix(esIndexPrefix + ".");
        indexStorage = esStorage;
        
        searchServer = startupSearchServer(esIndexPrefix, tempDirPath.resolve("SearchServiceTemp"),
                wsadmintoken, searchTypesDir, mappingsDir);
        
        searchCli = new KBaseSearchEngineClient(
                new URL("http://localhost:" + searchServer.getServerPort()), userToken);
        searchCli.setIsInsecureHttpConnectionAllowed(true);

        // create a search client without authentication to test
        // optional authentication for search API

        noAuthSearchCli = new KBaseSearchEngineClient(
                new URL("http://localhost:" + searchServer.getServerPort()));
        noAuthSearchCli.setIsInsecureHttpConnectionAllowed(true);
        
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

    protected static class ServerThread extends Thread {
        private KBaseSearchEngineServer server;
        
        protected ServerThread(KBaseSearchEngineServer server) {
            this.server = server;
        }
        
        public void run() {
            try {
                server.startupServer();
            } catch (Exception e) {
                System.err.println("Can't start server:");
                e.printStackTrace();
            }
        }
    }
    
    private static KBaseSearchEngineServer startupSearchServer(
            final String esIndexPrefix,
            final Path tempDir,
            final AuthToken indexerToken,
            final Path typesDir,
            final Path typeMappingDir)
            throws Exception {
        
        //write the server config file:
        File iniFile = File.createTempFile("test", ".cfg",
                new File(TestCommon.getTempDir()));
        if (iniFile.exists()) {
            iniFile.delete();
        }
        System.out.println("Created temporary config file: " + iniFile.getAbsolutePath());
        Ini ini = new Ini();
        Section searchini = ini.add("KBaseSearchEngine");
        searchini.add("auth-service-url", "http://localhost:" + auth.getServerPort() +
                "/testmode/api/legacy/KBase");
        searchini.add("auth-service-url-allow-insecure", "true");
        searchini.add("workspace-url", "http://localhost:" + ws.getServerPort());
        searchini.add("scratch", tempDir);
        searchini.add("indexer-token", indexerToken.getToken());
        searchini.add("elastic-host", "localhost");
        searchini.add("elastic-port", es.getServerPort());
        searchini.add("elastic-namespace", esIndexPrefix);
        searchini.add("types-dir", typesDir);
        searchini.add("type-mappings-dir", typeMappingDir);
        ini.store(iniFile);
        iniFile.deleteOnExit();
        
        //set up env
        Map<String, String> env = TestCommon.getenv();
        env.put("KB_DEPLOYMENT_CONFIG", iniFile.getAbsolutePath());

        final KBaseSearchEngineServer server = new KBaseSearchEngineServer();
        new ServerThread(server).start();
        System.out.println("Main thread waiting for search server to start up");
        while (server.getServerPort() == null) {
            Thread.sleep(1000);
        }
        return server;
    }
    
    @After
    public void init() throws Exception {
        TestCommon.destroyDB(wsdb);
        indexStorage.dropData();
        if (searchServer != null) {
            searchServer.stopServer();
        }
        searchServer = startupSearchServer(esIndexPrefix, tempDirPath.resolve("SearchServiceTemp"),
                wsadmintoken, searchTypesDir, mappingsDir);
        searchCli = new KBaseSearchEngineClient(
                new URL("http://localhost:" + searchServer.getServerPort()), userToken);
        searchCli.setIsInsecureHttpConnectionAllowed(true);

        noAuthSearchCli = new KBaseSearchEngineClient(
                new URL("http://localhost:" + searchServer.getServerPort()));
        noAuthSearchCli.setIsInsecureHttpConnectionAllowed(true);

    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        final boolean deleteTempFiles = TestCommon.getDeleteTempFiles();
        if (searchServer != null) {
            searchServer.stopServer();
        }
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

    @Test
    public void sourceTags() throws Exception {
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("sourceTags"));

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("SourceTags", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator")
                        .withSourceTag("refdata")
                        .withSourceTag("testnarr")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);
        
        final ObjectData expected1 = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid("WS:1/1/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withCreator("creator")
                .withType("SourceTags")
                .withTypeVer(1L)
                .withObjectName("objname1")
                .withTimestamp(10000L);
        
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("SourceTags", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname2", "creator")
                        .withSourceTag("refdata")
                        .withSourceTag("narrative")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/2/1"),
                ImmutableMap.of(new GUID("WS:1/2/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony")))),
                false);
        
        final ObjectData expected2 = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony")))
                .withGuid("WS:1/2/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony"))
                .withCreator("creator")
                .withType("SourceTags")
                .withTypeVer(1L)
                .withObjectName("objname2")
                .withTimestamp(10000L);
        
        final SearchObjectsOutput res1 = searchObjects(new MatchFilter()
                .withSourceTags(Arrays.asList("narrative")));
        
        assertThat("incorrect object count", res1.getObjects().size(), is(1));
        TestCommon.compare(res1.getObjects().get(0), expected2);
        
        final SearchObjectsOutput res2 = searchObjects(new MatchFilter()
                .withSourceTags(Arrays.asList("narrative"))
                .withSourceTagsBlacklist(1L));

        assertThat("incorrect object count", res2.getObjects().size(), is(1));
        TestCommon.compare(res2.getObjects().get(0), expected1);
    }

    @Test
    public void narrativeDecorationDisabled() throws Exception {
        final long wsdate = WorkspaceEventHandler.parseDateToEpochMillis(wsCli1.createWorkspace(
                new CreateWorkspaceParams()
                        .withWorkspace("decorate")
                        .withMeta(ImmutableMap.of(
                                "narrative", "6",
                                "narrative_nice_name", "Kevin")))
                .getE4());

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Deco", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);

        final ObjectData expected1 = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid("WS:1/1/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withCreator("creator")
                .withType("Deco")
                .withTypeVer(1L)
                .withObjectName("objname1")
                .withTimestamp(10000L);

        final SearchObjectsOutput res = searchObjects(new MatchFilter());

        // Narrative info set to null, as the postprocessing input is not given
        assertNull(res.getAccessGroupNarrativeInfo());
        assertNull(res.getAccessGroupsInfo());
        assertNull(res.getObjectsInfo());

        final SearchObjectsOutput res2 = searchCli.searchObjects(
                new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter())
                        .withAccessFilter(new AccessFilter())
                        .withPostProcessing(new PostProcessing().withAddNarrativeInfo(0L)));

        // Narrative info set to null, as the addNarrativeInfo flag was set to 0
        assertNull(res2.getAccessGroupNarrativeInfo());
        // workspaces and objects info set to null, by default
        assertNull(res2.getAccessGroupsInfo());
        assertNull(res2.getObjectsInfo());
    }

    @Test
    public void narrativeDecoration() throws Exception {

        final long wsdate = WorkspaceEventHandler.parseDateToEpochMillis(wsCli1.createWorkspace(
                new CreateWorkspaceParams()
                        .withWorkspace("decorate")
                        .withMeta(ImmutableMap.of(
                                "narrative", "6",
                                "narrative_nice_name", "Kevin")))
                .getE4());

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Deco", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);

        final ObjectData expected1 = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid("WS:1/1/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withCreator("creator")
                .withType("Deco")
                .withTypeVer(1L)
                .withObjectName("objname1")
                .withTimestamp(10000L);

        final SearchObjectsOutput res = searchCli.searchObjects(
                new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter())
                        .withAccessFilter(new AccessFilter())
                        .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L)));

        assertThat("incorrect object count", res.getObjects().size(), is(1));
        TestCommon.compare(res.getObjects().get(0), expected1);
        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = ImmutableMap.of(
                1L, narrInfoTuple("Kevin", 6L, wsdate, userToken.getUserName(), "display1"));
        NarrativeInfoDecoratorTest.compare(res.getAccessGroupNarrativeInfo(), expected);
    }

    private void setUpOptionalAuthForSearch() throws Exception{
        wsCli1.createWorkspace(new CreateWorkspaceParams().withWorkspace("optionalAuth1"));
        wsCli1.createWorkspace(new CreateWorkspaceParams().withWorkspace("optionalAuth2"));

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Deco", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);  // isPublic set to false

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Deco", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname2", "creator")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:2/1/1"),
                ImmutableMap.of(new GUID("WS:2/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                true);  // isPublic set to true
    }

    private ObjectData setUpOptionalAuthForSearchGetRes(String guid, String objName) throws Exception{
        return new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid(guid)
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withCreator("creator")
                .withType("Deco")
                .withTypeVer(1L)
                .withObjectName(objName)
                .withTimestamp(10000L)
                .withHighlight(new HashMap<>());
    }
    @Test
    public void optionalAuthSearchObjects() throws Exception{
        setUpOptionalAuthForSearch();

        final ObjectData expected1 = setUpOptionalAuthForSearchGetRes("WS:1/1/1","objname1");
        final ObjectData expected2 = setUpOptionalAuthForSearchGetRes("WS:2/1/1","objname2");

        SearchObjectsOutput authSearchObjsOut;
        SearchObjectsOutput noAuthSearchObjsOut;


        try {
            noAuthSearchObjsOut =  noAuthSearchCli.searchObjects(new SearchObjectsInput()
                    .withAccessFilter(new AccessFilter())
                    .withMatchFilter(new MatchFilter()));

            authSearchObjsOut =  searchCli.searchObjects(new SearchObjectsInput()
                    .withAccessFilter(new AccessFilter())
                    .withMatchFilter(new MatchFilter()));
        } catch (ServerException e) {
            System.out.println("Exception server side trace:\n" + e.getData());
            throw e;
        }

        assertThat("incorrect search objects count", noAuthSearchObjsOut.getObjects().size(), is(1));
        TestCommon.compare(noAuthSearchObjsOut.getObjects().get(0), expected2);

//        assertThat("incorrect search objects count", authSearchObjsOut.getObjects().size(), is(2));
//
//        Set<String> res = new HashSet<>();
//        res.add(expected1.getGuid());
//        res.add(expected2.getGuid());
//        assertThat("incorrect results", res.contains(authSearchObjsOut.getObjects().get(0).getGuid()), is(true));
//        assertThat("incorrect results", res.contains(authSearchObjsOut.getObjects().get(1).getGuid()), is(true));

        final Set<String> got = authSearchObjsOut.getObjects().stream()
                .map(o -> o.getGuid()).collect(Collectors.toSet());
        assertThat("incorrect results", got, is(set("WS:1/1/1", "WS:2/1/1")));

    }
    @Test
    public void optionalAuthForSearchTypes() throws Exception {
        setUpOptionalAuthForSearch();

        SearchTypesOutput authSearchTypesOut;
        SearchTypesOutput noAuthSearchTypesOut;

        try {
            noAuthSearchTypesOut = noAuthSearchCli.searchTypes(new SearchTypesInput()
                    .withMatchFilter(new MatchFilter())
                    .withAccessFilter(new AccessFilter()));

            authSearchTypesOut = searchCli.searchTypes(new SearchTypesInput()
                    .withMatchFilter(new MatchFilter())
                    .withAccessFilter(new AccessFilter()));

        } catch (ServerException e) {
            System.out.println("Exception server side trace:\n" + e.getData());
            throw e;
        }

        Map<String, Long> typeToCount = noAuthSearchTypesOut.getTypeToCount();
        assertThat("incorrect search types map size", typeToCount.size(), is(1));
        assertThat("Incorrect search types type name", typeToCount.keySet(), is(set("Deco")));
        assertThat("Incorrect search types type count", typeToCount.get("Deco"), is(Long.valueOf(1)));

        typeToCount = authSearchTypesOut.getTypeToCount();
        assertThat("incorrect search types map size", typeToCount.size(), is(1));
        assertThat("Incorrect search types type name", typeToCount.keySet(), is(set("Deco")));
        assertThat("Incorrect search types type count", typeToCount.get("Deco"), is(Long.valueOf(2)));
    }

    @Test
    public void optionalAuthForGetObjects() throws Exception {
        setUpOptionalAuthForSearch();

        GetObjectsOutput authGetObjsOut;
        GetObjectsOutput noAuthGetObjsOut;

        try {
            noAuthGetObjsOut = noAuthSearchCli.getObjects(new GetObjectsInput()
                    .withGuids(Arrays.asList("WS:1/1/1", "WS:2/1/1")));

            authGetObjsOut = searchCli.getObjects(new GetObjectsInput()
                    .withGuids(Arrays.asList("WS:1/1/1", "WS:2/1/1")));

        } catch (ServerException e) {
            System.out.println("Exception server side trace:\n" + e.getData());
            throw e;
        }

        assertThat("incorrect get objects count", noAuthGetObjsOut.getObjects().size(), is(0));
        assertThat("incorrect get objects count", authGetObjsOut.getObjects().size(), is(2));

    }

    @Test
    public void accessGroupInfoDecorationDisabled() throws Exception {

        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo1")
                .withMeta(ImmutableMap.of(
                        "narrative", "6",
                        "narrative_nice_name", "Kevin")));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo1")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "one")))
                                .withName("objname1")
                                .withType("NoIndexingRules.Type-1.0")
                ))
        );
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo2"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo2")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "two")))
                                .withName("objname2")
                                .withType("NoIndexingRules.Type-1.0")
                ))
        );
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("wsDeco1", 1),
                        new StorageObjectType("foo", "bar1"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee1"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator1")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee1\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee1", Arrays.asList("imaprettypony1")))),
                false);

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("wsDeco2", 1),
                        new StorageObjectType("foo", "bar2"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee2"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname2", "creator2")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:2/1/1"),
                ImmutableMap.of(new GUID("WS:2/1/1"), new ParsedObject(
                        "{\"whee2\": \"imaprettypony2\"}",
                        ImmutableMap.of("whee2", Arrays.asList("imaprettypony2")))),
                false);

        // test searchObjects. postprocessing input is not given
        final SearchObjectsOutput res = searchObjects(new MatchFilter());
        assertNull(res.getAccessGroupNarrativeInfo());
        assertNull(res.getAccessGroupsInfo());
        assertNull(res.getObjectsInfo());

        final SearchObjectsOutput searchResults = searchCli.searchObjects(
                new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter())
                        .withAccessFilter(new AccessFilter())
                        .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(0L)));

        // Narrative info not added, set to null by default
        assertNull(searchResults.getAccessGroupNarrativeInfo());
        // set to null, since the addAccessGroupInfo flag was set to 0
        assertNull(searchResults.getAccessGroupsInfo());
        assertNull(searchResults.getObjectsInfo());

        // test get objects. postprocessing input is not given
        GetObjectsOutput getObjResults = searchCli.getObjects(
                new GetObjectsInput()
                        .withGuids(Arrays.asList("WS:1/1/1", "WS:2/1/1")));
        assertNull(getObjResults.getAccessGroupNarrativeInfo());
        assertNull(getObjResults.getAccessGroupsInfo());
        assertNull(getObjResults.getObjectsInfo());

        getObjResults = searchCli.getObjects(
                new GetObjectsInput()
                        .withGuids(Arrays.asList("WS:1/1/1", "WS:2/1/1"))
                        .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(0L)));

        // Narrative info not added, set to null by default
        assertNull(getObjResults.getAccessGroupNarrativeInfo());
        // set to null, since the addAccessGroupInfo flag was set to 0
        assertNull(getObjResults.getAccessGroupsInfo());
        assertNull(getObjResults.getObjectsInfo());
    }

    @Test
    public void accessGroupInfoDecoration() throws Exception {

        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo1")
                .withMeta(ImmutableMap.of(
                        "narrative", "6",
                        "narrative_nice_name", "Kevin")));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo1")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "one")))
                                .withName("objname1")
                                .withType("NoIndexingRules.Type-1.0")
                ))
        );
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("foo2"));
        wsCli1.saveObjects(new SaveObjectsParams()
                .withWorkspace("foo2")
                .withObjects(Arrays.asList(
                        new ObjectSaveData()
                                .withData(new UObject(ImmutableMap.of(
                                        "whee", "wugga",
                                        "whoo", "thingy",
                                        "req", "two")))
                                .withName("objname2")
                                .withType("NoIndexingRules.Type-1.0")
                ))
        );
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("wsDeco1", 1),
                        new StorageObjectType("foo", "bar1"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee1"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator1")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee1\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee1", Arrays.asList("imaprettypony1")))),
                false);

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("wsDeco2", 1),
                        new StorageObjectType("foo", "bar2"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee2"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname2", "creator2")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:2/1/1"),
                ImmutableMap.of(new GUID("WS:2/1/1"), new ParsedObject(
                        "{\"whee2\": \"imaprettypony2\"}",
                        ImmutableMap.of("whee2", Arrays.asList("imaprettypony2")))),
                false);

        final SearchObjectsOutput searchResults = searchCli.searchObjects(
                new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter())
                        .withAccessFilter(new AccessFilter())
                        .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L)));

        final Tuple9<Long, String, String, String, Long, String, String, String,
                Map<String, String>> wsInfoExpected1 =
                wsTuple(1, "foo1", "user1", "date1", 1, "n", "n", "unlocked",
                        ImmutableMap.of("narrative", "6", "narrative_nice_name", "Kevin"));
        final Tuple9<Long, String, String, String, Long, String, String, String,
                Map<String, String>> wsInfoExpected2 =
                wsTuple(2, "foo2", "user1", "date2", 1, "n", "n", "unlocked", Collections.emptyMap());

        final Tuple11<Long, String, String, String, Long, String,
                Long, String, String, Long, Map<String, String>> objInfoExpected1 =
                objTuple(1, "objname1", "NoIndexingRules.Type-1.0", "date1",1,"user1",
                         1, "foo1", "chksum1", 44, Collections.emptyMap());

        final Tuple11<Long, String, String, String, Long, String,
                Long, String, String, Long, Map<String, String>> objInfoExpected2 =
                objTuple(1, "objname2", "NoIndexingRules.Type-1.0", "date2",1,"user1",
                        2, "foo2", "chksum2", 44, Collections.emptyMap());

        // verify the values in workspacesInfo map
        compareWsInfo(searchResults.getAccessGroupsInfo().get(1L), wsInfoExpected1);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(2L), wsInfoExpected2);

        // verify the values in objectsInfo map
        compareObjInfo(searchResults.getObjectsInfo().get("1/1/1"), objInfoExpected1);
        compareObjInfo(searchResults.getObjectsInfo().get("2/1/1"), objInfoExpected2);

        final GetObjectsOutput getObjResults = searchCli.getObjects(
                new GetObjectsInput()
                        .withGuids(Arrays.asList("WS:1/1/1", "WS:2/1/1"))
                        .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L)));

        // verify the values in workspacesInfo map
        compareWsInfo(getObjResults.getAccessGroupsInfo().get(1L), wsInfoExpected1);
        compareWsInfo(getObjResults.getAccessGroupsInfo().get(2L), wsInfoExpected2);

        // verify the values in objectsInfo map
        compareObjInfo(getObjResults.getObjectsInfo().get("1/1/1"), objInfoExpected1);
        compareObjInfo(getObjResults.getObjectsInfo().get("2/1/1"), objInfoExpected2);
    }

    @Test
    public void accessGroupInfoDecorationNoObjects() throws Exception {

        // test when search results is empty, getObjectsInfo3 is not called to get objectsInfo
        // and the mappings for workspaces info and objects info are empty
        final SearchObjectsOutput searchResults = searchCli.searchObjects(
                new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter())
                        .withAccessFilter(new AccessFilter())
                        .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L)));
        assertEquals(searchResults.getAccessGroupsInfo().size(), 0);
        assertEquals(searchResults.getObjectsInfo().size(), 0);
    }

    @Test
    public void highlightTest () throws Exception{
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("highlight"));

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("SourceTags", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator")
                        .withSourceTag("refdata")
                        .withSourceTag("testnarr")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);

        //default for highlighting is off -- mainly b/c of search tags
        final kbasesearchengine.PostProcessing pp = new kbasesearchengine.PostProcessing();
        //1L to get this to be true
        pp.setIncludeHighlight(1L);
        final MatchFilter filter = new MatchFilter().withFullTextInAll("objname1");

        Map<String, List<String>> highlight = new HashMap<>();
        highlight.put("object_name",  Arrays.asList("<em>objname1</em>"));
        final ObjectData expected = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid("WS:1/1/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withCreator("creator")
                .withType("SourceTags")
                .withTypeVer(1L)
                .withObjectName("objname1")
                .withHighlight(highlight)
                .withTimestamp(10000L);
        
        SearchObjectsInput params = new SearchObjectsInput()
                .withPostProcessing(pp)
                .withAccessFilter(new AccessFilter())
                .withMatchFilter(filter);

        SearchObjectsOutput res = searchCli.searchObjects(params);

        final ObjectData actual = res.getObjects().get(0);
        TestCommon.compare(actual, expected);

        //highlight in objects
        assertThat("incorrect highlight", actual.getHighlight(), is(expected.getHighlight()));

        //test b/c highlight is unable to find number/dates and may return null
        final MatchFilter filter2 = new MatchFilter().withFullTextInAll("WS:1/1/1");
        SearchObjectsInput params2 = new SearchObjectsInput()
                .withPostProcessing(pp)
                .withAccessFilter(new AccessFilter())
                .withMatchFilter(filter2);

        SearchObjectsOutput res2 = searchCli.searchObjects(params2);

        final ObjectData actual2 = res2.getObjects().get(0);
        TestCommon.compare(actual2, expected);
        assertThat("highlight should return empty map", actual2.getHighlight(), is(Collections.emptyMap()));
    }
    
    private SearchObjectsOutput searchObjects(final MatchFilter mf) throws Exception {
        try {
            return searchCli.searchObjects(new SearchObjectsInput()
                    .withAccessFilter(new AccessFilter())
                    .withMatchFilter(mf));
        } catch (ServerException e) {
            System.out.println("Exception server side trace:\n" + e.getData());
            throw e;
        }
    }

    @Test
    public void sort() throws Exception {
        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("sort"));

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Sort", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator1")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);
        
        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Sort", 1),
                        new StorageObjectType("foo", "bar"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("whee"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator2")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/2/1"),
                ImmutableMap.of(new GUID("WS:1/2/1"), new ParsedObject(
                        "{\"whee\": \"imaprettypony1\"}",
                        ImmutableMap.of("whee", Arrays.asList("imaprettypony1")))),
                false);
        
        final SearchObjectsOutput res = searchCli.searchObjects(new SearchObjectsInput()
                .withAccessFilter(new AccessFilter())
                .withMatchFilter(new MatchFilter())
                .withSortingRules(Arrays.asList(new SortingRule()
                        .withAscending(0L)
                        .withIsObjectProperty(0L)
                        .withProperty("creator"))));
        
        final List<String> guids = res.getObjects().stream().map(od -> od.getGuid()).collect(
                Collectors.toList());
        
        assertThat("incorrect order", guids, is(Arrays.asList("WS:1/2/1", "WS:1/1/1")));
        
        assertThat("incorrect sort rules count", res.getSortingRules().size(), is(1));
        
        final SortingRule sr = res.getSortingRules().get(0);
        
        assertThat("incorrect property", sr.getProperty(), is("creator"));
        assertThat("incorrect ascending", sr.getAscending(), is(0L));
        assertThat("incorrect is object property", sr.getIsObjectProperty(), is(0L));
    }

    @Test
    public void status() throws Exception {
        final Map<String, Object> res = searchCli.status();
        // since the test working relies on the presence of the git.properties doc, which may
        // or may not be present given on how the test is run, we just check that something
        // is returned for the git properties.
        assertThat("null git url", res.get("git_url"), is(notNullValue()));
        assertThat("null git commit", res.get("git_commit_hash"), is(notNullValue()));
        System.out.println(res);
        
        res.remove("git_url");
        res.remove("git_commit_hash");
        
        final Map<String, Object> expected = ImmutableMap.of(
                "state", "OK",
                "message", "",
                "version", "0.1.1");
        
        assertThat("incorrect status output", res, is(expected));
    }
    
    /* **** Narrative pruner tests ***
     * these should be removed once the narratives are indexed
     * with custom code
     */
    @Test
    public void pruneNarrative() throws Exception {

        wsCli1.createWorkspace(new CreateWorkspaceParams()
                .withWorkspace("narprune"));

        final Map<String, Object> parsedData = new HashMap<>();
        parsedData.put("source", "a long string");
        parsedData.put("code_output", "another long string");
        parsedData.put("app_output", "yet another long string");
        parsedData.put("app_info", "yup, another long string here");
        parsedData.put("app_input", "my god will this reign of long string terror every end");
        parsedData.put("job_ids", "3");
        parsedData.put("title", "a title");

        indexStorage.indexObjects(
                ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType("Narrative", 1),
                        new StorageObjectType("WS", "Narrative"))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("source"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("code_output"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("app_output"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("app_info"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("app_input"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("job_ids"))
                                .build())
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("title"))
                                .build())
                        .build(),
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname1", "creator1")
                        .build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID("WS:1/1/1"),
                ImmutableMap.of(new GUID("WS:1/1/1"), new ParsedObject(
                        new ObjectMapper().writeValueAsString(parsedData),
                        parsedData.entrySet().stream().collect(Collectors.toMap(
                                e -> e.getKey(), e -> Arrays.asList(e.getValue()))))),
                false);

        final GetObjectsOutput ret = searchCli.getObjects(new GetObjectsInput()
                .withGuids(Arrays.asList("WS:1/1/1"))
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L)));

        assertThat("incorrect data count", ret.getObjects().size(), is(1));
        final ObjectData od2 = ret.getObjects().get(0);
        assertThat("incorrect data", od2.getData(), is((UObject) null));
        assertThat("incorrect keyprops", od2.getKeyProps(),
                is(ImmutableMap.of("title", "a title")));

        final SearchObjectsOutput res = searchObjects(new MatchFilter());

        assertThat("incorrect data count", res.getObjects().size(), is(1));
        final ObjectData od = res.getObjects().get(0);
        assertThat("incorrect data", od.getData(), is((UObject) null));
        assertThat("incorrect keyprops", od.getKeyProps(),
                is(ImmutableMap.of("title", "a title")));
    }


    /* ****** Auth client tests - to be moved to their own suite ***
     *     //TODO TEST move the auth client tests to a separate suite
     * Will need a mock server to test cases where the client gets a response that would never
     * be returned from auth
     */
    @Test
    public void construct() throws Exception {
        final TemporaryAuth2Client client = new TemporaryAuth2Client(
                new URL("http://localhost:1000/whee"));
        
        assertThat("incorrect url", client.getURL(), is(new URL("http://localhost:1000/whee/")));
        
        final TemporaryAuth2Client client2 = new TemporaryAuth2Client(
                new URL("http://localhost:1000/whee/"));
        
        assertThat("incorrect url", client2.getURL(), is(new URL("http://localhost:1000/whee/")));
    }

    @Test
    public void authClientGetDisplayNames() throws Exception {

        final TemporaryAuth2Client client = new TemporaryAuth2Client(authURL);
        assertThat("incorrect users", client.getUserDisplayNames(
                userToken.getToken(), set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));
    }

    @Test
    public void authClientGetDisplayNamesEmptyInput() throws Exception {

        final TemporaryAuth2Client client = new TemporaryAuth2Client(authURL);
        assertThat("incorrect users", client.getUserDisplayNames(userToken.getToken(), set()),
                is(Collections.emptyMap()));
    }

    @Test
    public void authClientGetDisplayNamesServerError() throws Exception {

        final TemporaryAuth2Client client = new TemporaryAuth2Client(
                new URL("http://localhost:" + auth.getServerPort()));
        try {
            client.getUserDisplayNames(userToken.getToken(), set("Baduser"));
            fail("expected exception");
        } catch (Auth2Exception got) {
            TestCommon.assertExceptionCorrect(got, new Auth2Exception(
                    "Auth service returned error code 400 with call id " +
                    got.getCallID().get() + 
                    // there's some software gore for you
                    // https://github.com/kbase/auth2/blob/73160676e4b64c9316c1c93023e20514519744d7/src/us/kbase/auth2/service/api/Users.java#L68
                    ": 30010 Illegal user name: Illegal user name [Baduser]: " +
                    "30010 Illegal user name: Illegal character in user name Baduser: B"));
            //they really want you to know the user name is illegal
        }
    }

    @Test
    public void authClientFailConstruct() throws Exception {

        try {
            new TemporaryAuth2Client(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("authURL"));
        }
    }

    @Test
    public void authClientgetDisplayNamesBadInput() {

        failAuthClientGetDisplayNames(null, set(),
                new IllegalArgumentException("token cannot be null or whitespace only"));
        failAuthClientGetDisplayNames("   \t   \n ", set(),
                new IllegalArgumentException("token cannot be null or whitespace only"));
        
        failAuthClientGetDisplayNames("t", null, new NullPointerException("userNames"));
        failAuthClientGetDisplayNames("t", set("n", null),
                new IllegalArgumentException("Null or whitespace only entry in userNames"));
        failAuthClientGetDisplayNames("t", set("n", "  \t    \n  "),
                new IllegalArgumentException("Null or whitespace only entry in userNames"));
    }
    
    private void failAuthClientGetDisplayNames(
            final String token,
            final Set<String> userNames,
            final Exception expected) {
        try {
            new TemporaryAuth2Client(authURL).getUserDisplayNames(token, userNames);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}