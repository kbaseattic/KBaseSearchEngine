package kbasesearchengine.test.main;

import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.PostProcessing;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.authorization.AuthInfoProvider;
import kbasesearchengine.main.AccessGroupInfoDecorator;
import kbasesearchengine.main.NarrativeInfo;
import kbasesearchengine.main.NarrativeInfoDecorator;
import kbasesearchengine.main.NarrativeInfoProvider;
import kbasesearchengine.main.ObjectInfoProvider;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.WorkspaceInfoProvider;
import org.junit.Test;
import org.mockito.Mockito;
import us.kbase.common.service.Tuple5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessGroupInfoDecoratorTest {

    @Test
    public void searchObjectsWithDeletedWs() throws Exception {
        final AccessGroupInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
        final Pagination pag = new Pagination().withCount(3L).withStart(0L);
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L))
                .withPagination(pag);

        final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
        expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:4/2/1").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
        assertThat("incorrect number of hits looked at", res.getTotalInPage(), is(6L));
        compare( res.getAccessGroupNarrativeInfo().get(4L), narrInfoTuple("test", 1L, 1L, "user", null));
    }
    public AccessGroupInfoDecorator setUpSearchObjectsWithDeletedWs() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceInfoProvider wip = mock(WorkspaceInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, wip, oip);

        final Pagination pag = new Pagination().withCount(3L).withStart(0L);


        final ObjectData obj = new ObjectData().withGuid("WS:3/5/6").withCreator("user");
        final ObjectData obj2 = new ObjectData().withGuid("WS:4/1/7").withCreator("user");
        final ObjectData obj3 = new ObjectData().withGuid("WS:4/2/1").withCreator("user");


        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);
        objs.add(obj2);
        objs.add(obj3);

        //mock wip and oip returns

        when(search.searchObjects(Mockito.any(SearchObjectsInput.class), eq("user"))).thenReturn(getEmptySearchObjectsOutput()
                .withObjects(objs)
                .withTotal(10L)
                .withTotalInPage(3L)
                .withSearchTime(1L)
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withPagination(pag));

        return agid;
    }
    @Test

    public static void compareSearchObjectOutputRes(List<ObjectData> expected, List<ObjectData> res ){
        assertThat("incorrect number of results", expected.size(), is(res.size()));
        for(int i =0; i<Math.min(expected.size(), res.size()); i++){
            assertThat("incorrect object", expected.get(i).getGuid(), is(res.get(i).getGuid()));
        }
    }


    public static void compare(
            final Tuple5<String, Long, Long, String, String> got,
            final Tuple5<String, Long, Long, String, String> expected) {
        assertThat("incorrect narrative name", got.getE1(), is(expected.getE1()));
        assertThat("incorrect narrative id", got.getE2(), is(expected.getE2()));
        assertThat("incorrect epoch", got.getE3(), is(expected.getE3()));
        assertThat("incorrect owner", got.getE4(), is(expected.getE4()));
        assertThat("incorrect display name", got.getE5(), is(expected.getE5()));
    }

    public static Tuple5<String, Long, Long, String, String> narrInfoTuple(
            final String narrativeName,
            final Long narrativeId,
            final Long epoch,
            final String owner,
            final String displayName) {
        return new Tuple5<String, Long, Long, String, String>()
                .withE1(narrativeName)
                .withE2(narrativeId)
                .withE3(epoch)
                .withE4(owner)
                .withE5(displayName);
    }

    private SearchObjectsOutput getEmptySearchObjectsOutput(){
        return new SearchObjectsOutput()
                .withObjects(new ArrayList<>())
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withSearchTime(0L)
                .withTotal(0L)
                .withTotalInPage(0L)
                .withPagination(null)
                .withSortingRules(null);
    }

}
