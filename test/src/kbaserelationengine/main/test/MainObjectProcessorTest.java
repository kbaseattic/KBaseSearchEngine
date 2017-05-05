package kbaserelationengine.main.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.HttpHost;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.MongoClient;

import junit.framework.Assert;
import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.main.LineLogger;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.ObjectData;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;

public class MainObjectProcessorTest {
    private static final boolean cleanup = true;
    
    private static MainObjectProcessor mop = null;
    
    @BeforeClass
    public static void prepare() throws Exception {
        File testCfg = new File("test_local/test.cfg");
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
        String mongoHost = props.getProperty("secure.mongo_host");
        int mongoPort = Integer.parseInt(props.getProperty("secure.mongo_port"));
        String elasticHost = props.getProperty("secure.elastic_host");
        int elasticPort = Integer.parseInt(props.getProperty("secure.elastic_port"));
        String esUser = props.getProperty("secure.elastic_user");
        String esPassword = props.getProperty("secure.elastic_password");
        HttpHost esHostPort = new HttpHost(elasticHost, elasticPort);
        if (cleanup) {
            deleteAllTestMongoDBs(mongoHost, mongoPort);
            deleteAllTestElasticIndices(esHostPort, esUser, esPassword);
        }
        String kbaseEndpoint = props.getProperty("kbase_endpoint");
        URL wsUrl = new URL(kbaseEndpoint + "/ws");
        File typesDir = new File("resources/types");
        File tempDir = new File("test_local/temp_files");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String mongoDbName = "test_" + System.currentTimeMillis() + "_DataStatus";
        String esIndexPrefix = "test_" + System.currentTimeMillis() + ".";
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, mongoHost,
                mongoPort, mongoDbName, esHostPort, esUser, esPassword, esIndexPrefix, 
                typesDir, tempDir, false, new LineLogger() {
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
                }, null);
    }
    
    private static void deleteAllTestMongoDBs(String mongoHost, int mongoPort) {
        try (MongoClient mongoClient = new MongoClient(mongoHost, mongoPort)) {
            Iterable<String> it = mongoClient.listDatabaseNames();
            for (String dbName : it) {
                if (dbName.startsWith("test_")) {
                    System.out.println("Deleting Mongo database: " + dbName);
                    mongoClient.dropDatabase(dbName);
                }
            }
        }
    }
    
    private static void deleteAllTestElasticIndices(HttpHost esHostPort, String esUser,
            String esPassword) throws IOException {
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort, null);
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        for (String indexName : esStorage.listIndeces()) {
            if (indexName.startsWith("test_")) {
                System.out.println("Deleting Elastic index: " + indexName);
                esStorage.deleteIndex(indexName);
            }
        }
    }
    
    @Ignore
    @Test
    public void testGenomeManually() throws Exception {
        //mop.performOneTick();
        ObjectStatusEvent ev = new ObjectStatusEvent("-1", "WS", 20266, "2", 1, null, 
                System.currentTimeMillis(), "KBaseGenomes.Genome", ObjectStatusEventType.CREATED, false);
        mop.processOneEvent(ev);
        System.out.println("Genome: " + mop.getIndexingStorage("*").getObjectsByIds(
                mop.getIndexingStorage("*").searchIds("Genome", 
                        MatchFilter.create().withFullTextInAll("test"), null, 
                        AccessFilter.create().withAdmin(true), null).guids, null).get(0));
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
        ObjectData obj = mop.getIndexingStorage("*").getObjectsByIds(guids, null).get(0);
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
            System.out.println("DEBUG: " + mop.getIndexingStorage("*").getObjectsByIds(ids, null));
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
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 10455, "1", 78, null, 
                System.currentTimeMillis(), "KBaseNarrative.Narrative", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "Narrative", "Catalog.migrate_module_to_new_git_url", 10455, false);
        checkSearch(1, "Narrative", "Super password!", 10455, false);
        indexFewVersions(new ObjectStatusEvent("-1", "WS", 480, "1", 254, null, 
                System.currentTimeMillis(), "KBaseNarrative.Narrative", 
                ObjectStatusEventType.CREATED, false));
        checkSearch(1, "Narrative", "weird text", 480, false);
        checkSearch(1, "Narrative", "functionality", 480, false);
    }
    
    @Ignore
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
