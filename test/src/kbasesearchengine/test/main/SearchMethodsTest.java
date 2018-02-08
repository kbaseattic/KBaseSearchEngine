package kbasesearchengine.test.main;

import kbasesearchengine.AccessFilter;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.Pagination;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.authorization.AccessGroupProvider;
import kbasesearchengine.common.GUID;
import kbasesearchengine.main.SearchMethods;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.search.PostProcessing;
import kbasesearchengine.search.SortingRule;
import kbasesearchengine.system.TypeStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchMethodsTest {

    private final static PostProcessing PP_DEFAULT = new PostProcessing();
    static {
        PP_DEFAULT.objectData = true;
        PP_DEFAULT.objectInfo = true;
        PP_DEFAULT.objectKeys = true;
    }
    private final static IndexingStorage idx = mock(IndexingStorage.class);



    //TODO TEST add a whole lot more tests. Most of SearchMethods is not covered.
    
    @Test
    public void searchObjectsExcludeSubObjects() throws Exception {
        // false cases
        searchObjectsExcludeSubObjects(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(0L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchObjectsExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(2L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        //true case
        searchObjectsExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(1L),
                kbasesearchengine.search.MatchFilter.getBuilder().withExcludeSubObjects(true)
                        .build());
    }

    private void searchObjectsExcludeSubObjects(
            final MatchFilter input,
            final kbasesearchengine.search.MatchFilter expected)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);
        
        final SearchMethods sm = new SearchMethods(agp, is, ts, Collections.emptySet());
        
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
//        assertThat("incorrect objects", res.getSearchTime(), is(20));
        // don't care about the sorting rules for this test, so just check size
        assertThat("incorrect sorting rules count", res.getSortingRules().size(), is(1));
        assertThat("incorrect total", res.getTotal(), is(1L));
    }
    
    @Test
    public void searchTypesExcludeSubObjects() throws Exception {
        // false cases
        searchTypesExcludeSubObjects(
                new MatchFilter(),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(null),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(0L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        searchTypesExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(2L),
                kbasesearchengine.search.MatchFilter.getBuilder().build());
        
        //true case
        searchTypesExcludeSubObjects(
                new MatchFilter().withExcludeSubobjects(1L),
                kbasesearchengine.search.MatchFilter.getBuilder().withExcludeSubObjects(true)
                        .build());
    }

    private void searchTypesExcludeSubObjects(
            final MatchFilter input,
            final kbasesearchengine.search.MatchFilter expected)
            throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);
        
        final SearchMethods sm = new SearchMethods(agp, is, ts, Collections.emptySet());
        
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
//        assertThat("incorrect objects", res.getSearchTime(), is(20));
    }




    @Test
    public void searchObjectResult() throws Exception {
        final AccessGroupProvider agp = mock(AccessGroupProvider.class);
        final TypeStorage ts = mock(TypeStorage.class);
        final SearchMethods sm = new SearchMethods(agp, idx, ts, Collections.emptySet());


        final ArrayList<String> highlight = new ArrayList<>();
        highlight.add("<em>test</em>");

        final kbasesearchengine.search.ObjectData obj = kbasesearchengine.search.ObjectData
                .getBuilder(new GUID("ws:1/2/3"))
                .withHighlight("field", highlight).build();

        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);

        final PostProcessing pp = new PostProcessing();
        pp.objectHighlight = true;


        final SortingRule sr = new SortingRule();
        sr.isTimestamp = true;
        sr.ascending = true;

        final FoundHits fh = new FoundHits();
        fh.pagination = null;
        fh.sortingRules=  Arrays.asList(sr);;
        fh.total = 1;
        fh.guids = set(new GUID("ws:1/2/3"));
        fh.objects = objs;


        final kbasesearchengine.search.MatchFilter filter = kbasesearchengine.search.MatchFilter.getBuilder()
                .withNullableFullTextInAll("test")
                .build();

        when(idx.searchObjects(
                new ArrayList<>(),
                filter,
                null, // sort
                new kbasesearchengine.search.AccessFilter().withAccessGroups(set()),
                null, // pagination
                PP_DEFAULT))
                .thenReturn(fh);

        final SearchObjectsOutput res = sm.searchObjects(new SearchObjectsInput()
                        .withMatchFilter(new MatchFilter().withFullTextInAll("test"))
                        .withAccessFilter(new AccessFilter()), "auser");


        final Map<String, List<String>> highlightRes = new HashMap<>();
        highlightRes.put("field", highlight);

        assertThat("did not find objects", res.getObjects().size()>0, is(true));
        assertThat("did not find objects", res.getObjects().get(0).getHighlight(), is(highlightRes));

    }
}
