package kbaserelationengine.main.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import junit.framework.Assert;
import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.main.LineLogger;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.ObjectData;
import kbaserelationengine.search.PostProcessing;
import kbaserelationengine.test.common.TestCommon;
import kbaserelationengine.test.controllers.elasticsearch.ElasticSearchController;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import us.kbase.common.test.controllers.mongo.MongoController;

public class MainObjectProcessorTest {
	
    //TODO NOW tests use hard coded workspace
    //TODO NOW run workspace locally
	
    private static MainObjectProcessor mop = null;
    private static MongoController mongo;
    private static MongoClient mc;
    private static MongoDatabase db;
    
    private static Path tempDir;
    private static ElasticSearchController es;
    
    @BeforeClass
    public static void prepare() throws Exception {
        // set up mongo
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                Paths.get(TestCommon.getTempDir()),
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        final String dbName = "DataStatus";
        db = mc.getDatabase(dbName);
        
        // set up elastic search
        // should refactor to just use NIO at some point
        tempDir = Paths.get(TestCommon.getTempDir()).resolve("MainObjectProcessorTest");
        FileUtils.deleteQuietly(tempDir.toFile());
        tempDir.toFile().mkdirs();
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(),
                tempDir.resolve("ElasticSearchController"));
        
        // get other tests props
        // TODO NOW add to test common
        File testCfg = TestCommon.getConfigFilePath().toFile();
        Properties props = new Properties();
        try (InputStream is = new FileInputStream(testCfg)) {
            props.load(is);
        }
        URL authUrl = new URL(props.getProperty("auth_service_url"));
        String authAllowInsecure = props.getProperty("auth_service_url_allow_insecure");
        ConfigurableAuthService authSrv = new ConfigurableAuthService(
                new AuthConfig().withKBaseAuthServerURL(authUrl)
                .withAllowInsecureURLs("true".equals(authAllowInsecure)));
        String tokenStr = props.getProperty("secure.indexer_token");
        AuthToken kbaseIndexerToken = authSrv.validateToken(tokenStr);
        String kbaseEndpoint = props.getProperty("kbase_endpoint");
        URL wsUrl = new URL(kbaseEndpoint + "/ws");
        File typesDir = new File("resources/types");

        String esIndexPrefix = "test_" + System.currentTimeMillis() + ".";
        HttpHost esHostPort = new HttpHost("localhost", es.getServerPort());
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, "localhost",
                mongo.getServerPort(), dbName, esHostPort, null, null, esIndexPrefix, 
                typesDir, tempDir.resolve("MainObjectProcessor").toFile(), false,
                new LineLogger() {
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
                }, null);
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        if (mc != null) {
            mc.close();
        }
        if (mongo != null) {
            mongo.destroy(TestCommon.getDeleteTempFiles());
        }
        if (es != null) {
            es.destroy(TestCommon.getDeleteTempFiles());
        }
        if (tempDir != null && tempDir.toFile().exists() && TestCommon.getDeleteTempFiles()) {
            FileUtils.deleteQuietly(tempDir.toFile());
        }
    }
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(db);
    }
    
    @Test
    public void testGenomeManually() throws Exception {
        ObjectStatusEvent ev = new ObjectStatusEvent("-1", "WS", 20266, "2", 1, null, 
                System.currentTimeMillis(), "KBaseGenomes.Genome", ObjectStatusEventType.CREATED, false);
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

    private void indexFewVersions(ObjectStatusEvent ev) throws Exception {
        for (int i = Math.max(1, ev.getVersion() - 5); i <= ev.getVersion(); i++) {
            mop.processOneEvent(new ObjectStatusEvent(ev.getId(), ev.getStorageCode(), 
                    ev.getAccessGroupId(), ev.getAccessGroupObjectId(), i, 
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
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 20266, "1", 7, null, 
                System.currentTimeMillis(), "KBaseNarrative.Narrative", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "Narrative", "tree", 20266, false);
        checkSearch(1, "Narrative", "species", 20266, false);
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
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 20266, "5", 1, null, 
                System.currentTimeMillis(), "KBaseFile.PairedEndLibrary", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "PairedEndLibrary", "Illumina", 20266, true);
        checkSearch(1, "PairedEndLibrary", "sample1se.fastq.gz", 20266, false);
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 20266, "6", 1, null, 
                System.currentTimeMillis(), "KBaseFile.SingleEndLibrary", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "SingleEndLibrary", "PacBio", 20266, true);
        checkSearch(1, "SingleEndLibrary", "reads.2", 20266, false);
    }
    
    @Ignore
    @Test
    public void testOneTick() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println();
            long t1 = System.currentTimeMillis();
            mop.performOneTick(false);
            System.out.println("FULL TICK TIME: " + (System.currentTimeMillis() - t1) + " ms.");
            Thread.sleep(1000);
        }
    }
}
