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
import org.junit.Test;

import com.mongodb.MongoClient;

import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.search.ElasticIndexingStorage;
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
        HttpHost esHostPort = new HttpHost(elasticHost, elasticPort);
        if (cleanup) {
            deleteAllTestMongoDBs(mongoHost, mongoPort);
            deleteAllTestElasticIndices(esHostPort);
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
                mongoPort, mongoDbName, esHostPort, esIndexPrefix, typesDir, tempDir, false);
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
    
    private static void deleteAllTestElasticIndices(HttpHost esHostPort) throws IOException {
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort, null);
        for (String indexName : esStorage.listIndeces()) {
            if (indexName.startsWith("test_")) {
                System.out.println("Deleting Elastic index: " + indexName);
                esStorage.deleteIndex(indexName);
            }
        }
    }
    
    @Test
    public void testGenomeManually() throws Exception {
        //mop.performOneTick();
        ObjectStatusEvent ev = new ObjectStatusEvent("-1", "WS", 20266, "2", 1, null, 
                System.currentTimeMillis(), "KBaseGenomes.Genome", ObjectStatusEventType.CREATED, false);
        mop.processOneEvent(ev);
        System.out.println("Genome: " + mop.getIndexingStorage("*").getObjectsByIds(
                mop.getIndexingStorage("*").searchIdsByText("Genome", "test", null, null, true)).get(0));
        String query = "TrkA";
        Map<String, Integer> typeToCount = mop.getIndexingStorage("*").searchTypeByText(query, null, true);
        System.out.println("Counts per type: " + typeToCount);
        if (typeToCount.size() == 0) {
            return;
        }
        String type = typeToCount.keySet().iterator().next();
        Set<GUID> guids = mop.getIndexingStorage("*").searchIdsByText(type, query, null, null, true);
        System.out.println("GUIDs found: " + guids);
        ObjectData obj = mop.getIndexingStorage("*").getObjectsByIds(guids).get(0);
        System.out.println("Feature: " + obj);
    }
}
