package kbaserelationengine.main.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.MongoClient;

import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.MatchValue;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import workspace.ListObjectsParams;
import workspace.WorkspaceClient;

public class PerformanceTester {
    private static final boolean cleanup = true;
    
    private static AuthToken kbaseIndexerToken = null;
    private static URL wsUrl = null;
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
        kbaseIndexerToken = authSrv.validateToken(tokenStr);
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
        wsUrl = new URL(kbaseEndpoint + "/ws");
        File typesDir = new File("resources/types");
        File tempDir = new File("test_local/temp_files");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String mongoDbName = "test_" + System.currentTimeMillis() + "_DataStatus";
        String esIndexPrefix = "performance.";
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, mongoHost,
                mongoPort, mongoDbName, esHostPort, esUser, esPassword, esIndexPrefix, 
                typesDir, tempDir, false, null, null);
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
            if (indexName.startsWith("performance.")) {
                System.out.println("Deleting Elastic index: " + indexName);
                esStorage.deleteIndex(indexName);
            }
        }
    }
    
    @Ignore
    @Test
    public void testPerformance() throws Exception {
        String wsName = "ReferenceDataManager";
        WorkspaceClient wc = new WorkspaceClient(wsUrl, kbaseIndexerToken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        int blockPos = 0;
        int blockSize = 100;
        for (int n = 0; n < 100; n++, blockPos++) {
            System.out.println("\nProcessing block #" + blockPos);
            int genomesInit = 0;
            try {
                genomesInit = countGenomes();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            long minObjId = blockPos * blockSize + 1;
            long maxObjId = (blockPos + 1) * blockSize;
            List<String> refs = wc.listObjects(new ListObjectsParams().withWorkspaces(
                    Arrays.asList(wsName)).withMinObjectID(minObjId).withMaxObjectID(maxObjId)
                    .withType("KBaseGenomes.Genome"))
                    .stream().map(ObjectParser::getRefFromObjectInfo).collect(Collectors.toList());
            System.out.println("Refs: " + refs.size());
            long processTime = 0;
            for (String ref : refs) {
                String[] parts = ref.split("/");
                int wsId = Integer.parseInt(parts[0]);
                int version = Integer.parseInt(parts[2]);
                ObjectStatusEvent ev = new ObjectStatusEvent("-1", "WS", wsId, parts[1], version, null, 
                        System.currentTimeMillis(), "KBaseGenomes.Genome", ObjectStatusEventType.CREATED, false);
                long t2 = System.currentTimeMillis();
                try {
                    mop.processOneEvent(ev);
                    processTime += System.currentTimeMillis() - t2;
                } catch (Exception ex) {
                    System.out.println("Error: " + ex.getMessage());
                }
            }
            int genomes = countGenomes();
            System.out.println("Processing time: " + (((double)processTime) / (genomes - genomesInit)) + 
                    " ms. per genome (" + (genomes - genomesInit) + " genomes)");
            testCommonStats();
        }
    }
    
    private int countGenomes() throws Exception {
        return mop.getIndexingStorage("*").searchIds("Genome", 
                MatchFilter.create().withLookupInKey("features", new MatchValue(1, null)), null,
                AccessFilter.create().withPublic(true).withAdmin(true), null).total;
    }
    
    @Ignore
    @Test
    public void testCommonStats() throws Exception {
        String query = "transporter";
        int genomes = countGenomes();
        int features = mop.getIndexingStorage("*").searchTypes(MatchFilter.create(),
                AccessFilter.create().withPublic(true).withAdmin(true)).get("GenomeFeature");
        System.out.println("Total genomes/features processed: " + genomes + "/" + features);
        long t1 = System.currentTimeMillis();
        Map<String, Integer> typeToCount = mop.getIndexingStorage("*").searchTypes(
                MatchFilter.create().withFullTextInAll(query), 
                AccessFilter.create().withPublic(true).withAdmin(true));
        t1 = System.currentTimeMillis() - t1;
        System.out.println("Search time: " + t1 + " ms., features found: " + 
                typeToCount.get("GenomeFeature"));
    }
    
    @Ignore
    @Test
    public void parseResultsFeatures() throws Exception {
        List<String> lines =FileUtils.readLines(new File("test_local/performance.txt"));
        lines = lines.stream().filter(l -> l.contains("Total") || l.contains("Search")).collect(
                Collectors.toList());
        for (int i = 0; i < lines.size() / 2; i++) {
            String l1 = lines.get(i * 2);
            l1 = l1.split(" ")[3].split("/")[1];
            String l2 = lines.get(i * 2 + 1);
            l2 = l2.split(" ")[2];
            System.out.println(l1 + "\t" + l2);
        }
    }

    @Ignore
    @Test
    public void parseResultsGenomes() throws Exception {
        List<String> lines =FileUtils.readLines(new File("test_local/performance.txt"));
        lines = lines.stream().filter(l -> l.contains("Processing time") || l.contains("Total")).collect(
                Collectors.toList());
        for (int i = 0; i < lines.size() / 2; i++) {
            String l1 = lines.get(i * 2);
            l1 = l1.split(" ")[2];
            String l2 = lines.get(i * 2 + 1);
            l2 = l2.split(" ")[3].split("/")[0];
            System.out.println(l2 + "\t" + l1);
        }
    }
}
