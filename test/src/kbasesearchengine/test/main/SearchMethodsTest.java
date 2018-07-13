package kbasesearchengine.test.main;

import com.google.common.collect.ImmutableMap;
import kbasesearchengine.AccessFilter;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.Pagination;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SortingRule;
import kbasesearchengine.authorization.AccessGroupProvider;
import kbasesearchengine.common.GUID;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.SearchMethods;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchMethodsTest {

    private final static PostProcessing PP_DEFAULT = new PostProcessing();
    static {
        PP_DEFAULT.objectData = true;
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
        
        final kbasesearchengine.search.SortingRule sr = 
                kbasesearchengine.search.SortingRule.getStandardPropertyBuilder("timestamp")
                .build();
        
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
                .getBuilder(new GUID("ws:1/2/3"), new SearchObjectType("t", 1))
                .withHighlight("field", highlight).build();

        final kbasesearchengine.search.ObjectData obj2 = kbasesearchengine.search.ObjectData
                .getBuilder(new GUID("ws:4/5/6"), new SearchObjectType("t", 1))
                .build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final ArrayList<ObjectData> objs2 = new ArrayList<>();
        objs2.add(obj2);

        final kbasesearchengine.search.SortingRule sr =
                kbasesearchengine.search.SortingRule.getStandardPropertyBuilder("timestamp")
                .build();

        final kbasesearchengine.search.MatchFilter filter =
                kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("test")
                .build();

        //returns highlight. objectKeys,info,and data default to true
        final PostProcessing pp1 = new PostProcessing();
        pp1.objectHighlight = true;
        pp1.objectData = false;
        pp1.objectKeys = false;

        //returns nothing
        final PostProcessing pp2 = new PostProcessing();
        pp2.objectHighlight = false;
        pp2.objectData = false;
        pp2.objectKeys = false;

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
    public void objectDataConversion() throws Exception {
        final  IndexingStorage idx = mock(IndexingStorage.class);

        GUID guid = new GUID("ws:1/2/3");
        Instant time = Instant.now();

        //highlight already tested seperately. TODO add parent GUID && object
        //fields that are not added to kbasesearchengine.ObjectData are omitted for sanity
        final ObjectData obj = ObjectData.getBuilder(guid, new SearchObjectType("Blah" , 1))
                .withNullableTimestamp(time)
                .withNullableData("obj")
                .withNullableObjectName("objname")
                .withKeyProperty("key", "prop")
                .withNullableCreator("user")
                .withNullableCopier("user2")
                .withNullableModule("module")
                .withNullableMethod("method")
                .withNullableModuleVersion("2")
                .withNullableCommitHash("commitHash")
                .build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final kbasesearchengine.search.SortingRule sr =
                kbasesearchengine.search.SortingRule.getStandardPropertyBuilder("timestamp")
                .build();

        final kbasesearchengine.search.MatchFilter filter =
                kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("objname")
                .build();

        final PostProcessing pp1 = new PostProcessing();
        pp1.objectHighlight = false;
        pp1.objectData = true;
        pp1.objectKeys = true;


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
                .withSkipKeys(0L)
                .withSkipData(0L);

        final SearchObjectsOutput res1 = searchObjects(idx, option1, "objname");

        assertThat("did not find objects", res1.getObjects().size() == 1, is(true));
        kbasesearchengine.ObjectData actual = res1.getObjects().get(0);

        TestCommon.compare(actual, new kbasesearchengine.ObjectData()
                .withTimestamp(time.toEpochMilli())
                .withKeyProps(ImmutableMap.of("key", "prop"))
                .withData(new UObject("obj"))
                .withGuid(guid.toString())
                .withObjectName("objname")
                .withType("Blah")
                .withTypeVer(1L)
                .withCreator("user")
                .withCopier("user2")
                .withMod("module")
                .withMethod("method")
                .withModuleVer("2")
                .withCommit("commitHash"));
    }

    @Test
    public void objectDataConversionNullable() throws Exception {
        final  IndexingStorage idx = mock(IndexingStorage.class);
        GUID guid = new GUID("ws:1/2/3");

        final kbasesearchengine.search.ObjectData obj = kbasesearchengine.search.ObjectData
                .getBuilder(guid, new SearchObjectType("NoNulls", 1))
                .withNullableTimestamp(null)
                .withNullableData(null)
                .withNullableObjectName(null)
                .withNullableCreator(null)
                .withNullableCopier(null)
                .withNullableModule(null)
                .withNullableMethod(null)
                .withNullableModuleVersion(null)
                .withNullableCommitHash(null)
                .build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final kbasesearchengine.search.SortingRule sr =
                kbasesearchengine.search.SortingRule.getStandardPropertyBuilder("timestamp")
                .build();

        final kbasesearchengine.search.MatchFilter filter =
                kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("ws:1/2/3")
                .build();

        final PostProcessing pp1 = new PostProcessing();
        pp1.objectHighlight = false;
        pp1.objectData = true;
        pp1.objectKeys = false;

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
                .withSkipData(0L);

        final SearchObjectsOutput res1 = searchObjects(idx, option1, "ws:1/2/3");

        assertThat("did not find objects", res1.getObjects().size() == 1, is(true));
        kbasesearchengine.ObjectData actual = res1.getObjects().get(0);

        TestCommon.compare(actual, new kbasesearchengine.ObjectData()
                .withTimestamp(null)
                .withKeyProps(new HashMap<>())
                .withData(null)
                .withGuid(guid.toString())
                .withObjectName(null)
                .withType("NoNulls")
                .withTypeVer(1L)
                .withCreator(null)
                .withCopier(null)
                .withMod(null)
                .withMethod(null)
                .withModuleVer(null)
                .withCommit(null));
    }

    @Test
    public void sort() throws Exception {
        // tests that the sort inputs and outputs are correct.
        
        // minimal case
        sort(new SortingRule().withProperty("prop"),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop").build());
        
        // tests for is object prop
        sort(new SortingRule().withProperty("prop1").withIsObjectProperty(null),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop1").build());
        sort(new SortingRule().withProperty("prop2").withIsObjectProperty(1L),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop2").build());
        sort(new SortingRule().withProperty("prop3").withIsObjectProperty(0L),
                kbasesearchengine.search.SortingRule.getStandardPropertyBuilder("prop3").build());
        
        // tests for ascending
        sort(new SortingRule().withProperty("prop4").withAscending(null),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop4").build());
        sort(new SortingRule().withProperty("prop5").withAscending(1L),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop5").build());
        sort(new SortingRule().withProperty("prop6").withAscending(0L),
                kbasesearchengine.search.SortingRule.getKeyPropertyBuilder("prop6")
                        .withNullableIsAscending(false).build());
    }
    
    private void sort(
            final SortingRule input,
            final kbasesearchengine.search.SortingRule expected)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);

        final SearchInterface sm = new SearchMethods(agp, is, ts, Collections.emptySet());
        
        // what's returned doesn't matter, we're just checking that indexing storage gets the
        // right message
        
        final FoundHits fh = new FoundHits();
        fh.pagination = null;
        fh.sortingRules = Arrays.asList(expected);
        fh.total = 1;
        fh.guids = set();
        fh.objects = Collections.emptyList();
        
        when(is.searchObjects(
                Arrays.asList("Genome"),
                kbasesearchengine.search.MatchFilter.getBuilder().build(),
                Arrays.asList(expected), // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                PP_DEFAULT))
                .thenReturn(fh);
        
        final SearchObjectsOutput res = sm.searchObjects(new SearchObjectsInput()
                .withObjectTypes(Arrays.asList("Genome"))
                .withMatchFilter(new MatchFilter())
                .withAccessFilter(new AccessFilter())
                .withSortingRules(Arrays.asList(input)),
                "auser");

        assertThat("incorrect objects", res.getObjects(), is(Collections.emptyList()));
        assertThat("incorrect pagination", res.getPagination(), is((Pagination) null));
        // if we want to check search time, need to mock a Clock. Don't bother for now.
        // assertThat("incorrect objects", res.getSearchTime(), is(20));
        // don't care about the sorting rules for this test, so just check size
        assertThat("incorrect sorting rules count", res.getSortingRules().size(), is(1));
        assertThat("incorrect total", res.getTotal(), is(1L));
        
        compare(res.getSortingRules().get(0), input);
    }

    private void compare(final SortingRule got, final SortingRule expected) {
        assertThat("incorrect property", got.getProperty(), is(expected.getProperty()));
        assertThat("incorrect ascending", got.getAscending(),
                is(expected.getAscending() == null ? 1L : expected.getAscending()));
        assertThat("incorrect is object property", got.getIsObjectProperty(),
                is(expected.getIsObjectProperty() == null ? 1L : expected.getIsObjectProperty()));
    }

    private SearchInterface setUpTestAccessFilter () throws Exception{
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);

        final Set<String> admins = new HashSet<>(Arrays.asList("auser"));
        final SearchInterface sm = new SearchMethods(agp, is, ts, admins);

        final List<Integer> adminList = new ArrayList<>();
        adminList.add(2);
        final Map<String, Integer> privateData = new HashMap<>();
        privateData.put("test", 1);

        final Map<String, Integer> publicData = new HashMap<>();
        publicData.put("notdefault", 1);

        final Map<String, Integer> allData = new HashMap<>();
        allData.put("test", 1);
        allData.put("notdefault", 1);

        when(agp.findAccessGroupIds("auser"))
                .thenReturn(adminList);

        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(new LinkedHashSet(adminList))
                        .withAdmin(true)
                        .withPublic(true)))
                .thenReturn(allData);


        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(new LinkedHashSet(adminList))
                        .withAdmin(true)
                        .withPublic(false)))
                .thenReturn(privateData);


        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(set())
                        .withAdmin(true)
                        .withPublic(true)))
                .thenReturn(publicData);

        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(set())
                        .withPublic(true)))
                .thenReturn(publicData);

        //should not happen
        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(set())
                        .withPublic(false)))
                .thenReturn(new HashMap<>());
        return sm;
    }
    @Test
    public void testAccessFilterAuthorizedUser() throws Exception{
        SearchInterface sm = setUpTestAccessFilter();
        //expected private results: SearchMethods changes integer results to longs
        final Map<String, Long> privateData = new HashMap<>();
        privateData.put("test", 1L);

        final Map<String, Long> allData = new HashMap<>();
        allData.put("test", 1L);
        allData.put("notdefault", 1L);

        final Map<String, Long> publicData = new HashMap<>();
        publicData.put("notdefault", 1L);


        final SearchTypesOutput res = sm.searchTypes(new SearchTypesInput()
                        .withAccessFilter(new AccessFilter().withWithPrivate(1L).withWithPublic(1L))
                        .withMatchFilter(new MatchFilter()),
                "auser");

        assertThat("should show both public and private", res.getTypeToCount(), is(allData));


        final SearchTypesOutput res2 = sm.searchTypes(new SearchTypesInput()
                        .withAccessFilter(new AccessFilter().withWithPrivate(0L).withWithPublic(1L))
                        .withMatchFilter(new MatchFilter()),
                "auser");
        assertThat("should show only public", res2.getTypeToCount(), is(publicData));

        final SearchTypesOutput res3 = sm.searchTypes(new SearchTypesInput()
                        .withAccessFilter(new AccessFilter().withWithPrivate(1L).withWithPublic(0L))
                        .withMatchFilter(new MatchFilter()),
                "auser");
        assertThat("should show only private", res3.getTypeToCount(), is(privateData));


        final SearchTypesOutput res4 = sm.searchTypes(new SearchTypesInput()
                        .withAccessFilter(new AccessFilter().withWithPrivate(0L).withWithPublic(0L))
                        .withMatchFilter(new MatchFilter()),
                "auser");
        assertThat("should show only private", res4.getTypeToCount(), is(new HashMap<>()));

    }

    @Test
    public void testAccessFilterUnauthorizedUser() throws Exception{
        SearchInterface sm = setUpTestAccessFilter();
        final Map<String, Long> publicData = new HashMap<>();
        publicData.put("notdefault", 1L);

        //unauth users have public and private hardcoded so not testing the other cases
        final SearchTypesOutput res2 = sm.searchTypes(new SearchTypesInput()
                        .withAccessFilter(new AccessFilter().withWithPrivate(0L).withWithPublic(1L))
                        .withMatchFilter(new MatchFilter()),
                null);
        assertThat("incorrect counts", res2.getTypeToCount(), is(publicData));


    }

    @Test
    public void testAccessFilter() throws Exception{
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);

        final Set<String> admins = new HashSet<>(Arrays.asList("auser"));
        final SearchInterface sm = new SearchMethods(agp, is, ts, admins);

        final List<Integer> adminList = new ArrayList<>();
        adminList.add(2);
        final Map<String, Integer> exMap = new HashMap<>();
        exMap.put("test", 1);

        when(agp.findAccessGroupIds("auser"))
                .thenReturn(adminList);

        when(is.searchTypes( kbasesearchengine.search.MatchFilter.getBuilder().build(),
                new kbasesearchengine.search.AccessFilter()
                        .withAccessGroups(new LinkedHashSet(adminList))
                        .withAdmin(true)))
        .thenReturn(exMap);


        //expected private results: SearchMethods changes integer results to longs
        final Map<String, Long> expectedRes = new HashMap<>();
        expectedRes.put("test", 1L);

        final SearchTypesInput input1 = new SearchTypesInput()
                .withAccessFilter(new AccessFilter().withWithPrivate(1L))
                .withMatchFilter(new MatchFilter());

        //authorized and showing private data
        final SearchTypesOutput res = sm.searchTypes(input1,
                "auser");

        assertThat("incorrect res", res.getTypeToCount(), is(expectedRes));

        //unauthorized user
        final SearchTypesOutput res2 = sm.searchTypes(input1,
                null);
        assertThat("incorrect counts", res2.getTypeToCount(), is(Collections.emptyMap()));

        //authorized user with private data set to false
        final SearchTypesOutput res3 = sm.searchTypes(new SearchTypesInput()
                .withAccessFilter(new AccessFilter().withWithPrivate(0L))
                .withMatchFilter(new MatchFilter()),
                "auser");
        assertThat("incorrect counts", res3.getTypeToCount(), is(Collections.emptyMap()));

    }
}
