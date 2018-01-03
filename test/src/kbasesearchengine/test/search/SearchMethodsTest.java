package kbasesearchengine.test.search;

import static org.junit.Assert.*;

import kbasesearchengine.MatchFilter;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.main.SearchMethods;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.test.controllers.elasticsearch.ElasticSearchController;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import us.kbase.common.service.UObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;


public class SearchMethodsTest {

    private SearchMethods searchMethods;
    private static File tempDir = null;
    private static ElasticSearchController es;

    @Mock
    private IndexingStorage indexingStorage;

    @Mock
    private TypeStorage typeStorage;

    @Mock
    private KeywordParser.ObjectLookupProvider objLookup;

    @Before
    public void setUp() throws Exception {
        final Path tdir = Paths.get(TestCommon.getTempDir());
        tempDir = tdir.resolve("ElasticIndexingStorageTest").toFile();
        FileUtils.deleteQuietly(tempDir);
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tdir);
        indexingStorage = new ElasticIndexingStorage(
                new HttpHost("localhost", es.getServerPort()), tempDir);
        searchMethods = new SearchMethods(indexingStorage, typeStorage);
        prepareDatabase();
    }

    @After
    public void cleanUp() {
        // TODO: Destroy objects created by prepareDatabase()
    }

    private void indexObject(GUID id, String objectType, String json, String objectName,
                             Instant timestamp, String parentJsonValue, boolean isPublic,
                             List<IndexingRules> indexingRules)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {
        ParsedObject obj = KeywordParser.extractKeywords(objectType, json, parentJsonValue,
                indexingRules, objLookup, null);
        final SourceData data = SourceData.getBuilder(new UObject(json), objectName, "creator")
                .build();
        indexingStorage.indexObject(id, objectType, obj, data, timestamp, parentJsonValue,
                isPublic, indexingRules);
    }

    private void prepareDatabase() throws ObjectParseException {
        String objectType = "Simple";
        IndexingRules ir = new IndexingRules();
        ir.setPath(new ObjectJsonPath("prop1"));
        ir.setFullText(true);
        List<IndexingRules> indexingRules= Arrays.asList(ir);
        try {
            indexObject(new GUID("WS:11/1/1"), objectType, "{\"prop1\":\"multiWordInSearchMethod1 multiWordInSearchMethod2\"}", "multiword.1", Instant.now(), null,
                    true, indexingRules);
            indexObject(new GUID("WS:11/2/1"), objectType, "{\"prop1\":\"multiWordInSearchMethod2\"}", "multiword.2", Instant.now(), null,
                    true, indexingRules);
            indexObject(new GUID("WS:11/3/1"), objectType, "{\"prop1\":\"multiWordInSearchMethod1\"}", "multiword.3", Instant.now(), null,
                    true, indexingRules);
        } catch (Exception ignore) {
            // HTTP 409 Conflict when you run tests repeatedly due to
            // already indexed object with identical GUID
            // Should add an @After to clean this up
        }

    }

    @Test
    public void testGetObjectServer() throws Exception{

        final SearchObjectsInput params = new SearchObjectsInput();
        final MatchFilter filter = new MatchFilter();

        filter.withFullTextInAll("multiWordInSearchMethod1 multiWordInSearchMethod2");
        params.setMatchFilter(filter);
        SearchObjectsOutput output1 = searchMethods.searchObjects(params);

        filter.withFullTextInAll("multiWordInSearchMethod2");
        params.setMatchFilter(filter);
        SearchObjectsOutput output2 = searchMethods.searchObjects(params);

        filter.withFullTextInAll("multiWordInSearchMethod1");
        params.setMatchFilter(filter);
        SearchObjectsOutput output3 =searchMethods.searchObjects(params);

        int size1 = output1.getObjects().size();
        int size2 = output2.getObjects().size();
        int size3 = output3.getObjects().size();

        assertTrue(size1 > 0);
        assertTrue(size2 > 0);
        assertTrue(size3 > 0);

        assertTrue(size3 > size1);
        assertTrue(size2 > size1);




    }
}
