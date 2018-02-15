package kbasesearchengine.test.main;

import com.google.common.collect.ImmutableMap;
import kbasesearchengine.AccessFilter;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.Pagination;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.authorization.AccessGroupProvider;
import kbasesearchengine.common.GUID;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.SearchMethods;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
import kbasesearchengine.search.SortingRule;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.test.common.TestCommon;
import org.junit.Test;
import us.kbase.common.service.UObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchMethodsTest {

    private final static PostProcessing PP_DEFAULT = new PostProcessing();
    static {
        PP_DEFAULT.objectData = true;
        PP_DEFAULT.objectInfo = true;
        PP_DEFAULT.objectKeys = true;
    }

    //TODO TEST add a whole lot more tests. Most of SearchMethods is not covered.
    
    @Test
    public void searchObjectsExcludeSubObjects() throws Exception {
        // false cases
        searchObjectsCheckMatchFilter(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(0L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(2L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        //true case
        searchObjectsCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(1L),
                kbasesearchengine.search.MatchFilter.getBuilder().withExcludeSubObjects(true)
                        .build());
    }
    
    @Test
    public void searchObjectsSourceTags() throws Exception {
        // null cases
        searchObjectsCheckMatchFilter(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsCheckMatchFilter(
                new MatchFilter().withSourceTags(null).withSourceTagsBlacklist(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        // whitelist
        searchObjectsCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar")),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .build());
        
        // explicit whitelist
        searchObjectsCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar"))
                        .withSourceTagsBlacklist(0L),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .build());
        
        // blacklist
        searchObjectsCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar"))
                        .withSourceTagsBlacklist(1L),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .withIsSourceTagsBlackList(true)
                        .build());
    }
    
    @Test
    public void searchObjectsIllegalSourceTag() {
        failSearchObjectsSourceTag(null, new IllegalArgumentException(
                "sourceTag cannot be null or whitespace only"));
        failSearchObjectsSourceTag("  \t  \n  ", new IllegalArgumentException(
                "sourceTag cannot be null or whitespace only"));
    }
    
    private void failSearchObjectsSourceTag(final String tag, final Exception expected) {
        try {
            searchObjectsCheckMatchFilter(
                    new MatchFilter().withSourceTags(Arrays.asList("foo", tag)),
                    kbasesearchengine.search.MatchFilter.getBuilder().build());
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
        
    }

    private void searchObjectsCheckMatchFilter(
            final MatchFilter input,
            final kbasesearchengine.search.MatchFilter expected)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);

        final SearchInterface sm = new SearchMethods(agp, is, ts, Collections.emptySet());
        
        // what's returned doesn't matter, we're just checking that indexing storage gets the
        // right message
        
        final SortingRule sr = new SortingRule();
        sr.isTimestamp = true;
        sr.ascending = true;
        
        final FoundHits fh = new FoundHits();
        fh.pagination = null;
        fh.sortingRules = Arrays.asList(sr);
        fh.total = 1;
        fh.guids = set();
        fh.objects = Collections.emptyList();
        
        when(is.searchObjects(
                Arrays.asList("Genome"),
                expected,
                null, // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                PP_DEFAULT))
                .thenReturn(fh);
        
        final SearchObjectsOutput res = sm.searchObjects(new SearchObjectsInput()
                .withObjectTypes(Arrays.asList("Genome"))
                .withMatchFilter(input)
                .withAccessFilter(new AccessFilter()),
                "auser");

        assertThat("incorrect objects", res.getObjects(), is(Collections.emptyList()));
        assertThat("incorrect pagination", res.getPagination(), is((Pagination) null));
        // if we want to check search time, need to mock a Clock. Don't bother for now.
        // assertThat("incorrect objects", res.getSearchTime(), is(20));
        // don't care about the sorting rules for this test, so just check size
        assertThat("incorrect sorting rules count", res.getSortingRules().size(), is(1));
        assertThat("incorrect total", res.getTotal(), is(1L));
    }
    
    @Test
    public void searchTypesExcludeSubObjects() throws Exception {
        // false cases
        searchTypesCheckMatchFilter(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(0L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(2L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        //true case
        searchTypesCheckMatchFilter(
                new MatchFilter().withExcludeSubobjects(1L),
                kbasesearchengine.search.MatchFilter.getBuilder().withExcludeSubObjects(true)
                        .build());
    }
    
    @Test
    public void searchTypesSourceTags() throws Exception {
        // null cases
        searchTypesCheckMatchFilter(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesCheckMatchFilter(
                new MatchFilter().withSourceTags(null).withSourceTagsBlacklist(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        // whitelist
        searchTypesCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar")),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .build());
        
        // explicit whitelist
        searchTypesCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar"))
                        .withSourceTagsBlacklist(0L),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .build());
        
        // blacklist
        searchTypesCheckMatchFilter(
                new MatchFilter().withSourceTags(Arrays.asList("foo", "bar"))
                        .withSourceTagsBlacklist(1L),
                kbasesearchengine.search.MatchFilter.getBuilder()
                        .withSourceTag("foo")
                        .withSourceTag("bar")
                        .withIsSourceTagsBlackList(true)
                        .build());
    }
    
    @Test
    public void searchTypesIllegalSourceTag() {
        failSearchTypesSourceTag(null, new IllegalArgumentException(
                "sourceTag cannot be null or whitespace only"));
        failSearchTypesSourceTag("  \t  \n  ", new IllegalArgumentException(
                "sourceTag cannot be null or whitespace only"));
    }
    
    private void failSearchTypesSourceTag(final String tag, final Exception expected) {
        try {
            searchTypesCheckMatchFilter(
                    new MatchFilter().withSourceTags(Arrays.asList("foo", tag)),
                    kbasesearchengine.search.MatchFilter.getBuilder().build());
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
        
    }

    private void searchTypesCheckMatchFilter(
            final MatchFilter input,
            final kbasesearchengine.search.MatchFilter expected)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);

        final SearchInterface sm = new SearchMethods(agp, is, ts, Collections.emptySet());
        
        // what's returned doesn't matter, we're just checking that indexing storage gets the
        // right message
        
        when(is.searchTypes(
                expected,
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set())))
                .thenReturn(Collections.emptyMap());
        
        final SearchTypesOutput res = sm.searchTypes(new SearchTypesInput()
                .withMatchFilter(input)
                .withAccessFilter(new AccessFilter()),
                "auser");
        
        assertThat("incorrect counts", res.getTypeToCount(), is(Collections.emptyMap()));
        // if we want to check search time, need to mock a Clock. Don't bother for now.
        // assertThat("incorrect objects", res.getSearchTime(), is(20));
    }

    @Test
    public void searchObjectResult() throws Exception {
        final  IndexingStorage idx = mock(IndexingStorage.class);


        final ArrayList<String> highlight = new ArrayList<>();
        highlight.add("<em>test</em>");

        final kbasesearchengine.search.ObjectData obj = kbasesearchengine.search.ObjectData
                .getBuilder(new GUID("ws:1/2/3"))
                .withHighlight("field", highlight).build();

        final kbasesearchengine.search.ObjectData obj2 = kbasesearchengine.search.ObjectData
                .getBuilder(new GUID("ws:4/5/6"))
                .build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final ArrayList<ObjectData> objs2 = new ArrayList<>();
        objs2.add(obj2);

        final SortingRule sr = new SortingRule();
        sr.isTimestamp = true;
        sr.ascending = true;

        final kbasesearchengine.search.MatchFilter filter = kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("test")
                .build();

        //returns highlight. objectKeys,info,and data default to true
        final PostProcessing pp1 = new PostProcessing();
        pp1.objectHighlight = true;
        pp1.objectData = false;
        pp1.objectKeys = false;
        pp1.objectInfo = false;
        pp1.objectDataIncludes=null;

        //returns nothing
        final PostProcessing pp2 = new PostProcessing();
        pp2.objectHighlight = false;
        pp2.objectData = false;
        pp2.objectKeys = false;
        pp2.objectInfo = false;
        pp2.objectDataIncludes=null;

        final FoundHits fh1 = new FoundHits();
        fh1.pagination = null;
        fh1.sortingRules =  Arrays.asList(sr);
        fh1.total = 1;
        fh1.guids = set(new GUID("ws:1/2/3"));
        fh1.objects = objs;

        final FoundHits fh2 = new FoundHits();
        fh2.pagination = null;
        fh2.sortingRules =  Arrays.asList(sr);
        fh2.total = 1;
        fh2.guids = set(new GUID("ws:4/5/6"));
        fh2.objects = objs2;

        //result with highlight
        when(idx.searchObjects(
                new ArrayList<>(),
                filter,
                null, // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                pp1))
                .thenReturn(fh1);

        //result with no highlight
        when(idx.searchObjects(
                new ArrayList<>(),
                filter,
                null, // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                pp2))
                .thenReturn(fh2);


        //tests
        //with highlight on only
        kbasesearchengine.PostProcessing option1 = new kbasesearchengine.PostProcessing()
                .withIncludeHighlight(1L)
                .withSkipKeys(1L)
                .withSkipInfo(1L)
                .withSkipData(1L);

        final SearchObjectsOutput res1 = searchObjects(idx, option1, "test");

        final Map<String, List<String>> highlightRes = new HashMap<>();
        highlightRes.put("field", highlight);
        assertThat("did not find objects", res1.getObjects().size() == 1, is(true));
        assertThat("did not get right highlight",
                res1.getObjects().get(0).getHighlight(),
                is(highlightRes));

        //highlight on and ids on
        kbasesearchengine.PostProcessing option2 = new kbasesearchengine.PostProcessing()
                .withIncludeHighlight(1L)
                .withIdsOnly(1L);

        final SearchObjectsOutput res2 = searchObjects(idx, option2, "test");

        assertThat("did not find objects", res2.getObjects().size() == 1, is(true));
        assertThat("did not get right highlight",
                res2.getObjects().get(0).getHighlight(),
                is(Collections.emptyMap()));

        //highlight off and ids off
        kbasesearchengine.PostProcessing option3 = new kbasesearchengine.PostProcessing()
                .withIdsOnly(1L);

        final SearchObjectsOutput res3 = searchObjects(idx, option3, "test");

        assertThat("did not find objects", res3.getObjects().size() == 1, is(true));
        assertThat("did not get right highlight",
                res3.getObjects().get(0).getHighlight(),
                is(Collections.emptyMap()));

    }

    private SearchObjectsOutput searchObjects(
            final IndexingStorage idx,
            final kbasesearchengine.PostProcessing pp,
            final String query)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final TypeStorage ts = mock(TypeStorage.class);
        final SearchMethods sm = new SearchMethods(agp, idx, ts, Collections.emptySet());


        final SearchObjectsInput input = new SearchObjectsInput()
                .withPostProcessing(pp)
                .withMatchFilter(new MatchFilter().withFullTextInAll(query))
                .withAccessFilter(new AccessFilter());

        return  sm.searchObjects(input, "auser");
    }

    @Test
    public void ObjectDataConversion() throws Exception {
        final  IndexingStorage idx = mock(IndexingStorage.class);

        GUID guid = new GUID("ws:1/2/3");
        Instant time = Instant.now();

        //highlight already tested seperately. TODO add parent GUID && object
        final kbasesearchengine.search.ObjectData obj = kbasesearchengine.search.ObjectData
                .getBuilder(guid)
                .withNullableTimestamp(time)
                .withNullableData("obj")
                .withNullableObjectName("objname")
                .withKeyProperty("key", "prop")
                .withNullableType(new SearchObjectType("Blah" , 1))
                .withNullableCreator("user")
                .withNullableCopier("user2")
                .withNullableModule("module")
                .withNullableMethod("method")
                .withNullableModuleVersion("2")
                .withNullableCommitHash("commitHash")
                .build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final SortingRule sr = new SortingRule();
        sr.isTimestamp = true;
        sr.ascending = true;

        final kbasesearchengine.search.MatchFilter filter = kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("objname")
                .build();

        final PostProcessing pp1 = new PostProcessing();
        pp1.objectHighlight = false;
        pp1.objectData = false;
        pp1.objectKeys = false;
        pp1.objectInfo = true;
        pp1.objectDataIncludes=null;


        final FoundHits fh1 = new FoundHits();
        fh1.pagination = null;
        fh1.sortingRules =  Arrays.asList(sr);
        fh1.total = 1;
        fh1.guids = set(new GUID("ws:1/2/3"));
        fh1.objects = objs;

        //result with highlight
        when(idx.searchObjects(
                new ArrayList<>(),
                filter,
                null, // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                pp1))
                .thenReturn(fh1);

        //tests
        //with highlight on only
        kbasesearchengine.PostProcessing option1 = new kbasesearchengine.PostProcessing()
                .withIncludeHighlight(0L)
                .withSkipKeys(1L)
                .withSkipInfo(0L)
                .withSkipData(1L);

        final SearchObjectsOutput res1 = searchObjects(idx, option1, "objname");

        assertThat("did not find objects", res1.getObjects().size() == 1, is(true));
        kbasesearchengine.ObjectData actual = res1.getObjects().get(0);

        assertThat("incorrect data", actual.getData().toString(),
                is(new UObject("obj").toString()));
        assertThat("incorrect guid", actual.getGuid(), is(guid.toString()));
        Map <String, String> expectedKeyProps = new HashMap<>();

        expectedKeyProps.put("key", "prop");
        assertThat("incorrect key props", actual.getKeyProps(), is(expectedKeyProps));
        assertThat("incorrect obj name", actual.getObjectName(), is("objname"));
        assertThat("incorrect timestamp", actual.getTimestamp(), is(time.toEpochMilli()));

        //additional properties are: type, creator, copied, module, method, module_ver, commit
        final ImmutableMap<String,String> expected = ImmutableMap.<String,String>builder()
                .put("type", "Blah")
                .put("creator", "user")
                .put("copied", "user2")
                .put("module", "module")
                .put("method", "method")
                .put("module_ver", "2")
                .put("commit", "commitHash")
                .build();

        Map<String, String > additonalProperties = actual.getObjectProps();
        assertThat("incorrect add props", additonalProperties.size() != 0,
                is(true));
        for( String prop : additonalProperties.keySet()) {
            assertThat("key " + prop + " has correct value",
                    additonalProperties.get(prop), is(expected.get(prop)));
        }

    }

    }
