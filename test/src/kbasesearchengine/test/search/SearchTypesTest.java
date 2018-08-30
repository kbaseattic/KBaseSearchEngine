package kbasesearchengine.test.search;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableMap;

import junit.framework.Assert;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.parse.KeywordParser;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.parse.ObjectParser;
import kbasesearchengine.parse.ParsedObject;
import kbasesearchengine.parse.KeywordParser.ObjectLookupProvider;
import kbasesearchengine.search.AccessFilter;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.MatchFilter;
import kbasesearchengine.search.MatchFilter.Builder;
import kbasesearchengine.search.MatchValue;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingConflictException;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.test.controllers.elasticsearch.ElasticSearchController;
import org.slf4j.LoggerFactory;
import us.kbase.common.service.UObject;

public class SearchTypesTest {
    private static File tempDir = null;
    private static ObjectLookupProvider objLookup;
    private static ElasticSearchController es;

    public SearchTypesTest() {
    }

    public static ElasticIndexingStorage createIndexingStorage(Integer aggregationSize) 
        throws Exception {        
        ElasticIndexingStorage indexStorage;
        String indexNamePrefix = "test_" + System.currentTimeMillis() + ".";
        if (aggregationSize == null) {
                indexStorage = new ElasticIndexingStorage(
                        new HttpHost("localhost", es.getServerPort()), 
                        tempDir);
        } else {
                indexStorage = new ElasticIndexingStorage(
                        new HttpHost("localhost", es.getServerPort()), 
                        tempDir,
                        aggregationSize);
        }
        indexStorage.setIndexNamePrefix(indexNamePrefix);
        return indexStorage;
    }

    @BeforeClass
    public static void prepare() throws Exception {
        TestCommon.stfuLoggers();

        final Path tdir = Paths.get(TestCommon.getTempDir());

        tempDir = tdir.resolve("SearchTypesTest").toFile();
        tempDir.mkdirs();

        // Start up ES once for this set of tests. This is very time consuming
        // and resource intensive, so should only be done once per set of tests.
        es = new ElasticSearchController(TestCommon.getElasticSearchExe(), tdir);

        // TODO: I'd like to dummy this down as far as possible, but don't 
        // understand the object lookup mechanism well enough.
        // It appears be only used in KeywordParser.transform for guid and lookup
        // transform types, so can probably be set null in these tests?
        // Yes, it appears so.
        objLookup = null;
    }

    @After
    public void cleanup() throws Exception {
        ElasticIndexingStorage indexStorage = createIndexingStorage(null);
        indexStorage.dropData();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (es != null) {
            es.destroy(TestCommon.getDeleteTempFiles());
        }
        if (tempDir != null && tempDir.exists() && TestCommon.getDeleteTempFiles()) {
            FileUtils.deleteQuietly(tempDir);
        }
    }

    private static void indexObject(
            IndexingStorage indexStorage,
            final GUID id,
            final ObjectTypeParsingRules rule,
            final String json,
            final String objectName,
            final Instant timestamp,
            final String parentJsonValue,
            final boolean isPublic)
            throws IOException, ObjectParseException, IndexingException, InterruptedException,
                IndexingConflictException {
        ParsedObject obj = KeywordParser.extractKeywords(id, rule.getGlobalObjectType(), json,
                parentJsonValue, rule.getIndexingRules(), objLookup, null);
        final SourceData data = SourceData.getBuilder(new UObject(json), objectName, "creator")
                .build();
        indexStorage.indexObject(rule, data, timestamp, parentJsonValue, id, obj, isPublic);
    }
    
    public void indexOne(IndexingStorage indexStorage, Integer id, boolean isPublic) throws Exception {
        String suffix = id.toString();
        ObjectTypeParsingRules rules = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("SearchType" + suffix, 1),
                new StorageObjectType("StorageCode" + suffix, "StorageType" + suffix))
                        .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("foo"))
                                .build())
                .build();

        // This is necessary to satisfy GUID validation.
        String guidString = "FAKE:1/" + suffix + "/1";

        indexStorage.indexObjects(
                rules,
                SourceData.getBuilder(new UObject(new HashMap<>()), "objname", "creator").build(),
                Instant.ofEpochMilli(10000),
                null,
                new GUID(guidString),
                ImmutableMap.of(new GUID(guidString), new ParsedObject(
                        "{\"foo\": \"bar\"}",
                        ImmutableMap.of("foo", Arrays.asList("bar")))),
                isPublic);
    }

    /*
        searchTypesGreaterThan10() tests a query over 11 types with the
        default aggregation size, which should be 1000. 
        Without the default aggregation size set as such, the total number 
        of buckets returned is 10 and this test would fail. In fact,
        this is the precise number of types which trigerred the
        report of this error and the addition of the aggregation size.
    */
    @Test
    public void searchTypes_GreaterThan10TypesIndexed() throws Exception {
        int itemCount = 11;

        // Setting null ensures that the default aggregation value of
        // 1000 is used. (See tests below for setting that value.)
        IndexingStorage indexStorage = createIndexingStorage(null);

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < itemCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(null)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));
        
        assertThat("inccorect type count", typeCounts.size(), is(itemCount));
        assertThat("incorrect type summary", typeCounts, is(expected));
    }

    /*
        searchTypesJust10() uses the default aggregation size, as above, but
        indexes just 10 types, which is the natural limit for ES aggregations.
        As above, this should always succeed, and is included because should, for
        some reason, the aggregation size be removed, the above test will fail,
        and this one will succeed, signaling the return of this bug.
    */
    @Test
    public void searchTypes_Just10TypesIndexed() throws Exception {
        int itemCount = 10;

        IndexingStorage indexStorage = createIndexingStorage(null);

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < itemCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(null)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));
        
        assertThat("inccorect type count", typeCounts.size(), is(itemCount));       
        assertThat("incorrect type summary", typeCounts, is(expected));
    }

    /*
        searchTypesNone() tests the boundary condition of nothing indexed.
        The query should result in an empty set of types.
    */
    @Test
    public void searchTypes_NothingIndexed() throws Exception {
        int itemCount = 0;

        IndexingStorage indexStorage = createIndexingStorage(null);

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < itemCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(null)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));
        
        assertThat("inccorect type count", typeCounts.size(), is(itemCount));
        assertThat("incorrect type summary", typeCounts, is(expected));
    }

    /*
        searchTypesNothingFound() tests the boundary condition of applying
        a query which matches nothing. The aggregation set should be empty.
    */
    @Test
    public void searchTypes_NothingFound() throws Exception {
        int indexedItemsAndTypeCount = 5;
        int expectedItemCount = 0;
        String searchFor = "quidditch";

        IndexingStorage indexStorage = createIndexingStorage(null);

        for (int i = 0; i < indexedItemsAndTypeCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
        }

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < expectedItemCount; i += 1) {
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(searchFor)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));

        assertThat("incorrect type count", typeCounts.size(), is(expectedItemCount));
        assertThat("incorrect type summary", typeCounts, is(expected));
    }

    /*
        searchTypesExactlySize() tests a query over N types with an
        aggregation size of N. This should exactly the same number of
        types (15) returned as may possibly be returned (15).
    */
    @Test
    public void searchTypes_TypesExactlyAggregationSize() throws Exception {
        int itemCount = 15;
        int aggregationSize = 15;

        IndexingStorage indexStorage = createIndexingStorage(aggregationSize);

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < itemCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(null)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));
        
        assertThat("incorrect type count", typeCounts.size(), is(itemCount));                
        assertThat("incorrect type count", typeCounts, is(expected));
    }
   
    /*
        searchTypesGreaterThanSize() tests a query over N+1 types with an
        aggregation size of N. This should lead fewer (15) types returned in
        the aggregation than actually exist (16).
        This simulates the boundary condition for the error which motivated 
        the creation of this test suite.
    */
    @Test
    public void searchTypes_TypesGreaterThanAggregationSize() throws Exception {
        int itemCount = 16;
        int aggregationSize = 15;

        IndexingStorage indexStorage = createIndexingStorage(aggregationSize);

        ImmutableMap.Builder<String, Integer> expectedBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < itemCount; i += 1) {
                indexOne(indexStorage, new Integer(i), true);
                expectedBuilder.put("SearchType" + i, 1);
        }
        ImmutableMap<String, Integer> expected =  expectedBuilder.build();

        final Map<String, Integer> typeCounts = indexStorage.searchTypes(
                MatchFilter.getBuilder()
                        .withNullableFullTextInAll(null)
                        .withExcludeSubObjects(true)
                        .build(),
                AccessFilter.create()
                        .withPublic(true));
        
        assertThat("incorrect type count", typeCounts.size(), is(aggregationSize));
        assertThat("incorrect type count", typeCounts.size(), not(is(itemCount)));
        assertThat("unexpectly correct type count", typeCounts, not(is(expected)));
    }
}