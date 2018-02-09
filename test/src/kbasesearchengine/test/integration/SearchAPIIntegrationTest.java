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

import com.fasterxml.jackson.databind.deser.DataFormatReaders;
import kbasesearchengine.search.PostProcessing;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.AccessFilter;
import kbasesearchengine.KBaseSearchEngineClient;
import kbasesearchengine.KBaseSearchEngineServer;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.common.FileUtil;
import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.handler.SourceData;
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
import us.kbase.auth.AuthToken;
import us.kbase.common.service.ServerException;
import us.kbase.common.service.UObject;
import us.kbase.common.test.controllers.mongo.MongoController;
import us.kbase.test.auth2.authcontroller.AuthController;
import us.kbase.workspace.CreateWorkspaceParams;
import us.kbase.workspace.WorkspaceClient;

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
    private static MongoController mongo;
    private static AuthController auth;
    private static AuthToken userToken;
    private static ElasticSearchController es;
    private static ElasticIndexingStorage indexStorage;
    private static WorkspaceController ws;
    private static MongoDatabase wsdb;
    private static MongoClient mc;
    private static WorkspaceClient wsCli1;
    private static KBaseSearchEngineServer searchServer;
    private static KBaseSearchEngineClient searchCli;
    
    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();

        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("SearchAPIIntegrationTest");
        // should refactor to just use NIO at some point
        FileUtils.deleteQuietly(tempDirPath.toFile());
        tempDirPath.toFile().mkdirs();
        final Path searchTypesDir = Files.createDirectories(tempDirPath.resolve("searchtypes"));
        installSearchTypes(searchTypesDir);
        final Path mappingsDir = Files.createDirectories(tempDirPath.resolve("searchmappings"));
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
        wsdb = mc.getDatabase("IndexerIntegTestWSDB");
        
        URL wsUrl = new URL("http://localhost:" + ws.getServerPort());
        wsCli1 = new WorkspaceClient(wsUrl, userToken);
        wsCli1.setIsInsecureHttpConnectionAllowed(true);
        
        final String esIndexPrefix = "test_" + System.currentTimeMillis();
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
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(wsdb);
        indexStorage.dropData();
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
                .withObjectProps(ImmutableMap.of("creator", "creator"))
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
                .withObjectProps(ImmutableMap.of("creator", "creator"))
                .withObjectName("objname2")
                .withTimestamp(10000L);
        
        final SearchObjectsOutput res1 = searchObjects(new MatchFilter()
                .withSourceTags(Arrays.asList("narrative")));
        
        assertThat("incorrect object count", res1.getObjects().size(), is(1));
        compare(res1.getObjects().get(0), expected2);
        
        final SearchObjectsOutput res2 = searchObjects(new MatchFilter()
                .withSourceTags(Arrays.asList("narrative"))
                .withSourceTagsBlacklist(1L));
        
        assertThat("incorrect object count", res2.getObjects().size(), is(1));
        compare(res2.getObjects().get(0), expected1);




    }

    @Test
    public void highlightTest () throws Exception{
        //ws with auth
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

        final SearchObjectsOutput res1 = searchObjects(new MatchFilter()
                .withSourceTags(Arrays.asList("immaprettypony1")));
        //default for highlighting is off -- mainly b/c of search tags
        final kbasesearchengine.PostProcessing pp = new kbasesearchengine.PostProcessing();
        final MatchFilter filter = new MatchFilter().withFullTextInAll("imaprettypony1");
        //1L to get this to be true
        pp.setIncludeHighlight(1L);

        Map<String, List<String>> highlight = new HashMap<>();
        highlight.put("whee",  Arrays.asList("<em>imaprettypony1</em>"));
        final ObjectData expected = new ObjectData()
                .withData(new UObject(ImmutableMap.of("whee", "imaprettypony1")))
                .withGuid("WS:1/1/1")
                .withKeyProps(ImmutableMap.of("whee", "imaprettypony1"))
                .withObjectProps(ImmutableMap.of("creator", "creator"))
                .withObjectName("objname1")
                .withHighlight(highlight)
                .withTimestamp(10000L);


        SearchObjectsInput params = new SearchObjectsInput()
                .withPostProcessing(pp)
                .withAccessFilter(new AccessFilter())
                .withMatchFilter(filter);


        SearchObjectsOutput res = searchCli.searchObjects(params);

        final ObjectData actual = res.getObjects().get(0);
        compare(actual, expected);

        //highlight in objects
        assertThat("incorrect highlight", actual.getHighlight(), is(expected.getHighlight()));

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



    private void compare(final ObjectData got, final ObjectData expected) {
        // no hashcode and equals compiled into ObjectData
        // or UObject for that matter
        assertThat("incorrect add props", got.getAdditionalProperties(),
                is(Collections.emptyMap()));
        assertThat("incorrect data", got.getData().asClassInstance(Map.class),
                is(expected.getData().asClassInstance(Map.class)));
        assertThat("incorrect guid", got.getGuid(), is(expected.getGuid()));
        assertThat("incorrect key props", got.getKeyProps(), is(expected.getKeyProps()));
        assertThat("incorrect obj name", got.getObjectName(), is(expected.getObjectName()));
        assertThat("incorrect obj props", got.getObjectProps(), is(expected.getObjectProps()));
        if (got.getParentData() == null) {
            assertThat("incorrect parent data", got.getParentData(), is(expected.getParentData()));
        } else {
            assertThat("incorrect parent data", got.getParentData().asClassInstance(Map.class),
                    is(expected.getParentData().asClassInstance(Map.class)));
        }
        assertThat("incorrect parent guid", got.getParentGuid(), is(expected.getParentData()));
        assertThat("incorrect timestamp", got.getTimestamp(), is(expected.getTimestamp()));
    }
    
}
