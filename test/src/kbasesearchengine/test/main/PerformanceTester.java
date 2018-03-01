package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import kbasesearchengine.common.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.ini4j.Ini;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.search.AccessFilter;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.MatchFilter;
import kbasesearchengine.search.MatchValue;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.system.TypeMappingParser;
import kbasesearchengine.system.YAMLTypeMappingParser;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import us.kbase.workspace.ListObjectsParams;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.WorkspaceIdentity;

/** NOTE: extremely large refactors have occurred since this code was written and it has not been
 * updated. It should be considered to be obsolete until someone takes the time to review and
 * rewrite the code for the new infrastructure.
 * 
 *
 */
public class PerformanceTester {
    private static final boolean cleanup = false;
    private static final boolean debug = false;
    
    private static AuthToken kbaseIndexerToken = null;
    private static String elasticHost = null;
    private static int elasticPort = -1;
    private static String esUser = null;
    private static String esPassword = null;
    private static File tempDir = null;
    private static URL wsUrl = null;
    private static IndexerWorker mop = null;
    private static IndexingStorage storage = null;
    private static List<long[]> timeStats = new ArrayList<>();
    
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
        Map<String, String> config = ini.get("KBaseSearchEngine");
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
            deleteAllTestElasticIndices(esHostPort, esUser, esPassword);
        }
        final Path typesDir = Paths.get("resources/types");
        final Path mappingsDir = Paths.get("resources/typemappings");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String esIndexPrefix = "performance.";
        final LineLogger logger = new LineLogger() {
            @Override
            public void logInfo(String line) {
                if (debug) {
                    System.out.println(line);
                }
            }
            @Override
            public void logError(String line) {
                if (debug) {
                    System.err.println(line);
                }
            }
            @Override
            public void logError(Throwable error) {
                if (debug) {
                    error.printStackTrace();
                }
            }
            @Override
            public void timeStat(GUID guid, long loadMs, long parseMs, long indexMs) {
                timeStats.add(new long[] {loadMs, parseMs, indexMs});
            }
        };
        final Map<String, TypeMappingParser> parsers = ImmutableMap.of(
                "yaml", new YAMLTypeMappingParser());
        final TypeStorage ss = new TypeFileStorage(typesDir, mappingsDir,
                new ObjectTypeParsingRulesFileParser(), parsers, new FileLister(), logger);
        
        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort,
                FileUtil.getOrCreateSubDir(tempDir, "esbulk"));
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        esStorage.setIndexNamePrefix(esIndexPrefix);
        storage = esStorage;
        mop = new IndexerWorker("test", Arrays.asList(), mock(StatusEventStorage.class),
                storage, ss, tempDir, logger, new HashSet<>(), 1000);
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
    
    @Test
    public void testPerformance() throws Exception {
        String wsName = "ReferenceDataManager";
        WorkspaceClient wc = new WorkspaceClient(wsUrl, kbaseIndexerToken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        int commonObjCount = (int)(long)wc.getWorkspaceInfo(
                new WorkspaceIdentity().withWorkspace(wsName)).getE5();
        int blockPos = 463;
        int blockSize = 100;
        int blockCount = (commonObjCount + blockSize - 1) / blockSize;
        System.out.println("Number of blocks: " + blockCount);
        for (; blockPos < blockCount; blockPos++) {
            timeStats.clear();
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
                final StoredStatusEvent ev = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                        new StorageObjectType("WS", "KBaseGenomes.Genome"),
                        Instant.now(),
                        StatusEventType.NEW_VERSION)
                        .withNullableAccessGroupID(wsId)
                        .withNullableObjectID(parts[1])
                        .withNullableVersion(version)
                        .withNullableisPublic(true)
                        .build(),
                        new StatusEventID("-1"),
                        StatusEventProcessingState.UNPROC)
                        .build();
                long t2 = System.currentTimeMillis();
                try {
                    mop.processOneEvent(ev.getEvent());
                    processTime += System.currentTimeMillis() - t2;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.out.println("Error: " + ex.getMessage());
                }
            }
            int genomes = countGenomes();
            double avgTime = ((double)processTime) / (genomes - genomesInit);
            long loadTotalTime = 0;
            long indexTotalTime = 0;
            for (long[] statRow : timeStats) {
                loadTotalTime += statRow[0];
                indexTotalTime += statRow[2];
            }
            double loadTime = ((double)loadTotalTime) / (genomes - genomesInit);
            double indexTime = ((double)indexTotalTime) / (genomes - genomesInit);
            System.out.println("Processing time: " + avgTime + " ms. (load: " + loadTime + 
                    ", index: " + indexTime + ") per genome (" + (genomes - genomesInit) + 
                    " genomes)");
            testCommonStats();
        }
    }
    
    private int countGenomes() throws Exception {
        return storage.searchIds(ImmutableList.of("Genome"),
                MatchFilter.getBuilder().withLookupInKey("features", new MatchValue(1, null))
                        .build(),
                null,
                AccessFilter.create().withPublic(true).withAdmin(true), null).total;
    }
    
    @Ignore
    @Test
    public void testCommonStats() throws Exception {
        String query = "Bacteria";
        int genomes = countGenomes();
        Map<String, Integer> typeAggr = storage.searchTypes(
                MatchFilter.getBuilder().build(), AccessFilter.create().withPublic(true)
                .withAccessGroups(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Integer features = typeAggr.get("GenomeFeature");
        Integer contigs = typeAggr.get("AssemblyContig");
        System.out.println("Total genomes/contigs/features processed: " + genomes + "/" + 
                contigs + "/" + features);
        long t1 = System.currentTimeMillis();
        Map<String, Integer> typeToCount = storage.searchTypes(
                MatchFilter.getBuilder().withNullableFullTextInAll(query).build(), 
                AccessFilter.create().withPublic(true)
                .withAccessGroups(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
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
        lines = lines.stream().filter(l -> l.contains("Processing time") || l.contains("Total"))
                .collect(Collectors.toList());
        for (int i = 0; i < lines.size() / 2; i++) {
            String l1 = lines.get(i * 2);
            l1 = l1.split(" ")[2];
            String l2 = lines.get(i * 2 + 1);
            l2 = l2.split(" ")[3].split("/")[0];
            System.out.println(l2 + "\t" + l1);
        }
    }

    @Test
    public void parseResultsAll() throws Exception {
        List<String> lines =FileUtils.readLines(new File("test_local/performance_real.txt"));
        lines = lines.stream().filter(l -> l.contains("Processing time") || l.contains("Total") ||
                l.contains("Search")).collect(
                Collectors.toList());
        System.out.println("#Gnms\t#Cntgs\t#Feats\tIndex\tLoad\tSave\tAvgIdx\tSearch\tFtFound");
        double totalIndexTime = 0;
        for (int i = 0; i < lines.size() / 3; i++) {
            String[] l1 = lines.get(i * 3).split(" ");
            double indexTime = Double.parseDouble(l1[2]);
            int blockSizeTokenPos = l1.length == 12 ? 10 : 6;
            int blockSize = Integer.parseInt(l1[blockSizeTokenPos].replace('(', ' ').trim());
            if (blockSize == 0) {
                continue;
            }
            double loadTime = l1.length == 12 ? Double.parseDouble(l1[5].replace(',', ' ').trim()) : -1;
            double saveTime = l1.length == 12 ? Double.parseDouble(l1[7].replace(')', ' ').trim()) : -1;
            totalIndexTime += indexTime * blockSize;
            String[] counts = lines.get(i * 3 + 1).split(" ")[3].split("/");
            int genomes = Integer.parseInt(counts[0]);
            String contigs = counts[1];
            String features = counts[2];
            String[] l3 = lines.get(i * 3 + 2).split(" ");
            String searchTime = l3[2];
            String featuresFound = l3[6];
            System.out.println(genomes + "\t" + contigs + "\t" + features + "\t" + 
                    (int)indexTime + "\t" + (int)loadTime + "\t" + (int)saveTime + "\t" + 
                    (int)(totalIndexTime / genomes) + "\t" + searchTime + "\t" + featuresFound);
        }
    }

}
