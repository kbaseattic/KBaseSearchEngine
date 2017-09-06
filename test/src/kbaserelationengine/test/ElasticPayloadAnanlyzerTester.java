package kbaserelationengine.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kbaserelationengine.search.ElasticIndexingStorage;
import us.kbase.common.service.UObject;

public class ElasticPayloadAnanlyzerTester {
    public static final boolean cleanup = true;
    
    private static String indexName;
    private static ElasticIndexingStorage indexStorage;
    
    @BeforeClass
    public static void prepare() throws Exception {
        String indexNamePrefix = "test_" + System.currentTimeMillis() + ".";
        indexStorage = new ElasticIndexingStorage(
                new HttpHost("localhost", 9200), null);
        indexStorage.setIndexNamePrefix(indexNamePrefix);
        cleanup();
        indexName = indexNamePrefix + "sparse";
        createTables(indexName);
    }
    
    @SuppressWarnings("serial")
    private static void createTables(String indexName) throws IOException {
        // Index settings
        Map<String, Object> payloadAnalyzer = new LinkedHashMap<String, Object>() {{
            put("type", "custom");
            put("tokenizer", "whitespace");
            put("filter", "delimited_payload_filter");
        }};
        Map<String, Object> analyzer = new LinkedHashMap<String, Object>() {{
            put("payload_analyzer", payloadAnalyzer);
        }};
        Map<String, Object> analysis = new LinkedHashMap<String, Object>() {{
            put("analyzer", analyzer);
        }};
        Map<String, Object> settings = new LinkedHashMap<String, Object>() {{
            put("analysis", analysis);
        }};
        // Table
        Map<String, Object> mappings = new LinkedHashMap<>();
        // Now data (child)
        String tableName = "sparse";
        Map<String, Object> table = new LinkedHashMap<>();
        mappings.put(tableName, table);
        Map<String, Object> props = new LinkedHashMap<>();
        table.put("properties", props);
        props.put("guid", new LinkedHashMap<String, Object>() {{
            put("type", "integer");
        }});
        props.put("@profile", new LinkedHashMap<String, Object>() {{
            put("type", "text");
            put("term_vector", "with_positions_offsets_payloads");
            put("analyzer", "payload_analyzer");
        }});
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("settings", settings);
            put("mappings", mappings);
        }};
        indexStorage.makeRequest("PUT", "/" + indexName, doc);
    }

    
    @AfterClass
    public static void teardown() throws Exception {
    }
    
    private static void cleanup() throws Exception {
        Set<String> indNames = indexStorage.listIndeces();
        for (String index : indNames) {
            /*if (!index.startsWith("test_")) {
                System.out.println("Skipping Elasticsearch index: " + index);
                continue;
            }*/
            System.out.println("Deleting Elasticsearch index: " + index);
            indexStorage.deleteIndex(index);
        }
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    @Test
    public void testMain() throws Exception {
        Random rand = new Random(1234567890);
        int datasetSize = 1000;
        int dimensions = 1000000;
        int fulfillCount = 1000;
        List<List<Integer>> generated = generatePackedData(rand, 
                datasetSize, dimensions, fulfillCount);
        try {
        File tempFile = new File("test_local/temp_files/esbulk/bulk.json");
        Map<String, Object> index = new LinkedHashMap<String, Object>() {{
            put("_index", indexName);
            put("_type", "sparse");
        }};
        Map<String, Object> header = new LinkedHashMap<String, Object>() {{
            put("index", index);
        }};
        List<Integer> firstRow = null;
        long t1 = System.currentTimeMillis();
        for (int blockPos = 0; blockPos < 1; blockPos++) {
            PrintWriter pw = new PrintWriter(tempFile);
            List<List<Integer>> callback = new ArrayList<>();
            //int saved = loadData(new File("test_local/sparse/data/sparse.csv"), 10000, blockPos,
            //        header, pw, callback);
            int saved = saveGeneratedBlock(generated, 10000, blockPos, header, pw, callback);
            pw.close();
            if (firstRow == null) {
                firstRow = callback.get(0);
            }
            System.out.println("Block " + blockPos + ": " + saved);
            if (saved == 0) {
                break;
            }
            indexStorage.makeBulkRequest("POST", "/" + indexName, tempFile);
        }
        indexStorage.makeRequest("POST", "/" + indexName + "/_refresh", null);
        System.out.println("Indexing is done in " + (System.currentTimeMillis() - t1) + " ms");
        List<Double> queryVec = new ArrayList<>();
        StringBuilder queryText = new StringBuilder();
        for (int i = 0; i < dimensions; i++) {
            queryVec.add(0.0);
        }
        for (int item : firstRow) {
            queryVec.set(item, 1.0);
            queryText.append(item).append(' ');
        }
        System.out.println("Query text: " + queryText.toString());
        Map<String, Object> queryString = new LinkedHashMap<String, Object>() {{
            put("query", queryText.toString());  // "*");
        }};
        Map<String, Object> subQuery = new LinkedHashMap<String, Object>() {{
            put("query_string", queryString);
        }};
        Map<String, Object> params = new LinkedHashMap<String, Object>() {{
            put("field", "@profile");
            put("vector", queryVec);  //Arrays.asList(0.1,2.3,-1.6,0.7,-1.3));
            put("cosine", true);
        }};
        Map<String, Object> script = new LinkedHashMap<String, Object>() {{
            put("inline", "payload_vector_score");
            put("lang", "native");
            put("params", params);
        }};
        Map<String, Object> scriptScore = new LinkedHashMap<String, Object>() {{
            put("script", script);
        }};
        Map<String, Object> functionScore = new LinkedHashMap<String, Object>() {{
            put("query", subQuery);
            put("script_score", scriptScore);
            put("boost_mode", "replace");
        }};
        Map<String, Object> query = new LinkedHashMap<String, Object>() {{
            put("function_score", functionScore);
        }};
        Map<String, Object> doc = new LinkedHashMap<String, Object>() {{
            put("query", query);
            put("_source", Arrays.asList("guid"));
        }};
        long t2 = System.currentTimeMillis();
        Response resp = indexStorage.makeRequest("GET", "/" + indexName + "/sparse/_search", doc);
        System.out.println("Search is done in " + (System.currentTimeMillis() - t2) + " ms");
        Map<String, Object> data = UObject.getMapper().readValue(
                resp.getEntity().getContent(), Map.class);
        Map<String, Object> hits = (Map<String, Object>)data.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>)hits.get("hits");
        for (Map<String, Object> hit : hitList) {
            double score = (Double)hit.get("_score");
            Map<String, Object> source = (Map<String, Object>)hit.get("_source");
            int guid = (Integer)source.get("guid");
            System.out.println("Found: " + guid + " -> " + score);
        }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        /*
        int datasetSize = 10000;
        int dimensions = 100000;
        int fulfillCount = 1000;
        Indexing is done in 10552 ms
        Search is done in 61158 ms
        */
    }
    
    private static int saveGeneratedBlock(List<List<Integer>> generated, 
            int blockSize, int blockPos, Map<String, Object> header,
            PrintWriter pw, List<List<Integer>> callback) throws Exception {
        int ret = 0;
        for (int i = blockPos * blockSize; i < Math.min((blockPos + 1) * blockSize, generated.size()); i++, ret++) {
            pw.println(UObject.transformObjectToString(header));
            pw.println(UObject.transformObjectToString(createDoc(i + 1, toProfile(generated.get(i)))));
        }
        callback.add(generated.get(0));
        return ret;
    }
    
    private static List<List<Integer>> generatePackedData(Random rand, 
            int datasetSize, int dimensions, int fulfillCount) {
        List<List<Integer>> ret = new ArrayList<>();
        byte[] v1 = new byte[dimensions];
        {
            for (int i = 0; i < fulfillCount; i++) {
                while (true) {
                    int pos = rand.nextInt(dimensions);
                    if (v1[pos] == 0) {
                        v1[pos] = 1;
                        break;
                    }
                }
            }

        }
        ret.add(pack(v1));
        for (int n = 1; n < datasetSize; n++) {
            byte[] v2 = new byte[dimensions];
            System.arraycopy(v1, 0, v2, 0, dimensions);
            int pos0 = findRandomPos(rand, v1, false);
            int pos1 = findRandomPos(rand, v1, true);
            if (v1[pos0] > 0 || v1[pos1] == 0) {
                throw new IllegalStateException();
            }
            v2[pos0] = 1;
            v2[pos1] = 0;
            ret.add(pack(v2));
            v1 = v2;
        }
        return ret;
    }
    
    private static int findRandomPos(Random rand, byte[] vec, boolean nonZero) {
        int dimensions = vec.length;
        int ret = rand.nextInt(dimensions);
        for (int iter = 0; (vec[ret] > 0) != nonZero; iter++) {
            ret = (ret + 1) % dimensions;
            if (iter >= dimensions) {
                throw new IllegalStateException("Too many iterations");
            }
        }
        return ret;
    }

    private static List<Integer> pack(byte[] array) {
        List<Integer> ret = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            if (array[i] != 0) {
                ret.add(i);
            }
        }
        return ret;
    }
    
    private static String toProfile(List<Integer> nonZeroPack) {
        StringBuilder ret = new StringBuilder();
        for (int pos : nonZeroPack) {
            if (ret.length() > 0) {
                ret.append(" ");
            }
            ret.append(pos).append("|").append(1);
        }
        return ret.toString();
    }
    
    @SuppressWarnings("unused")
    private static int loadData(File input, int blockSize, int blockPos, 
            Map<String, Object> header, PrintWriter pw, List<List<Integer>> firstRow) throws Exception {
        int blockStart = blockPos * blockSize;
        int itemsStoredInBlock = 0;
        int rowPos = 0;
        BufferedReader br = new BufferedReader(new FileReader(input));
        int currentId = -1;
        StringBuilder temp = null;
        List<Integer> row = new ArrayList<>();
        while (true) {
            String l = br.readLine();
            if (l == null || l.trim().length() == 0) {
                break;
            }
            String[] parts = l.split(Pattern.quote("\t"));
            int id = (int)Math.round(Double.parseDouble(parts[0]));
            if (id != currentId) {
                if (currentId > 0) {
                    if (rowPos >= blockStart) {
                        pw.println(UObject.transformObjectToString(header));
                        pw.println(UObject.transformObjectToString(createDoc(currentId, temp.toString())));
                        if (firstRow != null && firstRow.isEmpty()) {
                            firstRow.add(row);
                        }
                        currentId = -1;
                        itemsStoredInBlock++;
                        if (itemsStoredInBlock >= blockSize) {
                            break;
                        }
                    }
                    rowPos++;
                }
                currentId = id;
                temp = new StringBuilder();
                row = new ArrayList<>();
            }
            int profId = Integer.parseInt(parts[1]);
            if (temp.length() > 0) {
                temp.append(" ");
            }
            temp.append(profId).append("|").append(1);
            row.add(profId);
        }
        if (currentId > 0 && rowPos >= blockStart) {
            pw.println(UObject.transformObjectToString(header));
            pw.println(UObject.transformObjectToString(createDoc(currentId, temp.toString())));
            itemsStoredInBlock++;
        }
        br.close();
        return itemsStoredInBlock;
    }
    
    @SuppressWarnings("serial")
    private static Map<String, Object> createDoc(int guid, String profile) {
        return new LinkedHashMap<String, Object>() {{
            put("guid", guid);
            put("@profile", profile);
        }};
    }
}
