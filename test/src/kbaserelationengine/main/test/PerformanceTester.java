package kbaserelationengine.main.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.ini4j.Ini;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.MongoClient;

import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.main.LineLogger;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.search.AccessFilter;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.search.MatchFilter;
import kbaserelationengine.search.MatchValue;
import kbaserelationengine.search.ObjectData;
import kbaserelationengine.search.PostProcessing;
import kbaserelationengine.system.WsUtil;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import workspace.ListObjectsParams;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;

public class PerformanceTester {
    private static final boolean cleanup = true;
    
    private static AuthToken kbaseIndexerToken = null;
    private static String mongoHost = null;
    private static int mongoPort = -1;
    private static String elasticHost = null;
    private static int elasticPort = -1;
    private static String esUser = null;
    private static String esPassword = null;
    private static File tempDir = null;
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
        mongoHost = props.getProperty("secure.mongo_host");
        mongoPort = Integer.parseInt(props.getProperty("secure.mongo_port"));
        elasticHost = props.getProperty("secure.elastic_host");
        elasticPort = Integer.parseInt(props.getProperty("secure.elastic_port"));
        esUser = props.getProperty("secure.elastic_user");
        esPassword = props.getProperty("secure.elastic_password");
        String kbaseEndpoint = props.getProperty("kbase_endpoint");
        wsUrl = new URL(kbaseEndpoint + "/ws");
        tempDir = new File("test_local/temp_files");
        prepareStep2();
    }
    
    public static void main(String[] args) throws Exception {
        String KB_DEP = "KB_DEPLOYMENT_CONFIG";
        final String file = System.getProperty(KB_DEP) == null ?
                System.getenv(KB_DEP) : System.getProperty(KB_DEP);
        final File deploy = new File(file);
        final Ini ini = new Ini(deploy);
        Map<String, String> config = ini.get("KBaseRelationEngine");
        wsUrl = new URL(config.get("workspace-url"));
        String tokenStr = config.get("indexer-token");
        final String authURL = config.get("auth-service-url");
        final AuthConfig c = new AuthConfig();
        if ("true".equals(config.get("auth-service-url-allow-insecure"))) {
            c.withAllowInsecureURLs(true);
        }
        c.withKBaseAuthServerURL(new URL(authURL));
        ConfigurableAuthService auth = new ConfigurableAuthService(c);
        kbaseIndexerToken = auth.validateToken(tokenStr);
        mongoHost = config.get("mongo-host");
        mongoPort = Integer.parseInt(config.get("mongo-port"));
        elasticHost = config.get("elastic-host");
        elasticPort = Integer.parseInt(config.get("elastic-port"));
        esUser = config.get("elastic-user");
        esPassword = config.get("elastic-password");
        tempDir = new File(config.get("scratch"));
        prepareStep2();
        new PerformanceTester().testPerformance();
    }
    
    private static void prepareStep2() throws Exception {
        HttpHost esHostPort = new HttpHost(elasticHost, elasticPort);
        if (cleanup) {
            deleteAllTestMongoDBs(mongoHost, mongoPort);
            deleteAllTestElasticIndices(esHostPort, esUser, esPassword);
        }
        File typesDir = new File("resources/types");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String mongoDbName = "test_" + System.currentTimeMillis() + "_DataStatus";
        String esIndexPrefix = "performance.";
        LineLogger logger = null;  // getDebugLogger();
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, mongoHost,
                mongoPort, mongoDbName, esHostPort, esUser, esPassword, esIndexPrefix, 
                typesDir, tempDir, false, logger, null);
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
    
    private static ObjectData getIndexedObject(GUID guid) throws Exception {
        PostProcessing pp = new PostProcessing();
        pp.objectInfo = true;
        pp.objectKeys = true;
        pp.objectData = true;
        return mop.getIndexingStorage("*").getObjectsByIds(new LinkedHashSet<>(
                Arrays.asList(guid)), pp).get(0);
    }

    @Test
    public void testPerformance() throws Exception {
        String wsName = "ReferenceDataManager";
        WorkspaceClient wc = new WorkspaceClient(wsUrl, kbaseIndexerToken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        int commonObjCount = (int)(long)wc.getWorkspaceInfo(new WorkspaceIdentity().withWorkspace(wsName)).getE5();
        int blockPos = 0;
        int blockSize = 100;
        int blockCount = (commonObjCount + blockSize - 1) / blockSize;
        System.out.println("Number of blocks: " + blockCount);
        for (int n = 0; n < blockCount; n++, blockPos++) {
            long minObjId = blockPos * blockSize + 1;
            long maxObjId = Math.min(commonObjCount, (blockPos + 1) * blockSize);
            if (minObjId > maxObjId) {
                break;
            }
            System.out.println("\nProcessing block #" + blockPos);
            int genomesInit = 0;
            try {
                genomesInit = countGenomes();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            List<String> refs = wc.listObjects(new ListObjectsParams().withWorkspaces(
                    Arrays.asList(wsName)).withMinObjectID(minObjId).withMaxObjectID(maxObjId)
                    .withType("KBaseGenomes.Genome"))
                    .stream().map(WsUtil::getRefFromObjectInfo).collect(Collectors.toList());
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
                    /*System.out.println("Genome " + ev.toGUID() + ": time=" + (System.currentTimeMillis() - t2));
                    ObjectData genomeIndex = getIndexedObject(ev.toGUID());
                    String features = genomeIndex.keyProps.get("features");
                    String assemblyGuidText = genomeIndex.keyProps.get("assembly_guid");
                    ObjectData assemblyIndex = getIndexedObject(new GUID(assemblyGuidText));
                    String contigs = assemblyIndex.keyProps.get("contigs");
                    System.out.println("\tcontigs: " + contigs + ", features: " + features);*/
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.out.println("Error: " + ex.getMessage());
                }
            }
            int genomes = countGenomes();
            System.out.println("Processing time: " + (((double)processTime) / (genomes - genomesInit)) + 
                    " ms. per genome (" + (genomes - genomesInit) + " genomes)");
            testCommonStats();
        }
    }
    
    public static LineLogger getDebugLogger() {
        return new LineLogger() {
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
        };
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
        Map<String, Integer> typeAggr = mop.getIndexingStorage("*").searchTypes(
                MatchFilter.create(), AccessFilter.create().withPublic(true).withAdmin(true));
        Integer features = typeAggr.get("GenomeFeature");
        Integer contigs = typeAggr.get("AssemblyContig");
        System.out.println("Total genomes/contigs/features processed: " + genomes + "/" + 
                contigs + "/" + features);
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
