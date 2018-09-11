package kbasesearchengine.test.main;

import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.PostProcessing;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.common.GUID;
import kbasesearchengine.main.AccessGroupInfoDecorator;
import kbasesearchengine.main.ObjectInfoProvider;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.WorkspaceInfoProvider;
import org.junit.Test;
import org.mockito.Mockito;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessGroupInfoDecoratorTest {

    @Test
    public void searchObjectsWithNoAccessWs() throws Exception {
        final HashMap<Long, Tuple9> expectedRes = new HashMap<>();
        expectedRes.put(4L, AccessInfoTuple(4L, "test", "owner", "1", 5L, "r", "n", "unlocked", new HashMap()));
        expectedRes.put(5L, AccessInfoTuple(5L, "test", "owner", "1", 5L, "n", "r", "unlocked", new HashMap()));
        expectedRes.put(6L, AccessInfoTuple(6L, "test", "owner", "1", 5L, "r", "r", "unlocked", new HashMap()));
        final AccessGroupInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
        final Pagination pag = new Pagination().withCount(4L).withStart(0L);
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L))
                .withPagination(pag);

        final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
        expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:5/2/1").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:6/2/1").withCreator("user"));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
        compareAccessGroupInfo((HashMap) res.getAccessGroupsInfo(), expectedRes);
    }

    @Test
    public void searchObjectsWithNoAccesssWithLogging() throws Exception {
        final HashMap<Long, Tuple9> expectedRes = new HashMap<>();
        expectedRes.put(4L, AccessInfoTuple(4L, "test", "owner", "1", 5L, "r", "n", "unlocked", new HashMap()));
        expectedRes.put(5L, AccessInfoTuple(5L, "test", "owner", "1", 5L, "n", "r", "unlocked", new HashMap()));

        final AccessGroupInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
        final Pagination pag = new Pagination().withCount(2L).withStart(0L);
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L))
                .withPagination(pag);

        final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
        expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:5/2/1").withCreator("user"));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
        compareAccessGroupInfo((HashMap) res.getAccessGroupsInfo(), expectedRes);
    }


    public AccessGroupInfoDecorator setUpSearchObjectsWithDeletedWs() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceInfoProvider wip = mock(WorkspaceInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, wip, oip);

        final Pagination pag = new Pagination().withCount(4L).withStart(0L);


        final ObjectData obj = new ObjectData().withGuid("WS:3/5/6").withCreator("user");
        final ObjectData obj2 = new ObjectData().withGuid("WS:4/1/7").withCreator("user");
        final ObjectData obj3 = new ObjectData().withGuid("WS:5/2/1").withCreator("user");
        final ObjectData obj4 = new ObjectData().withGuid("WS:6/2/1").withCreator("user");


        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);
        objs.add(obj2);
        objs.add(obj3);
        objs.add(obj4);


        //mock wip and oip returns

        //private and no access
        when(wip.getWorkspaceInfo(3L)).thenReturn(
                AccessInfoTuple(3L, "test", "otheruser", "1", 5L, "n", "n", "unlocked", new HashMap())
        );
        //private with access
        when(wip.getWorkspaceInfo(4L)).thenReturn(
                AccessInfoTuple(4L, "test", "owner", "1", 5L, "r", "n", "unlocked", new HashMap())
        );
        //public with no access
        when(wip.getWorkspaceInfo(5L)).thenReturn(
                AccessInfoTuple(5L, "test", "owner", "1", 5L, "n", "r", "unlocked", new HashMap())
        );
        //public with access
        when(wip.getWorkspaceInfo(6L)).thenReturn(
                AccessInfoTuple(6L, "test", "owner", "1", 5L, "r", "r", "unlocked", new HashMap())
        );

        final List<String> guidList = Arrays.asList("WS:4/1/7","WS:5/2/1","WS:6/2/1");
        Map<String, Tuple11<Long, String, String, String, Long,
                String, Long, String, String, Long, Map<String, String>>> objsInfo = new HashMap<>();
        for(String guid : guidList){
            objsInfo.put(guid, new Tuple11<>());
        }

        when(oip.getObjectsInfo(guidList)).thenReturn(objsInfo);

        when(search.searchObjects(any(SearchObjectsInput.class), eq("user"))).thenReturn(getEmptySearchObjectsOutput()
                .withObjects(objs)
                .withTotal(10L)
                .withSearchTime(1L)
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withPagination(pag));

        return agid;
    }

    public static void compareSearchObjectOutputRes(List<ObjectData> expected, List<ObjectData> res ){
        assertThat("incorrect number of results", expected.size(), is(res.size()));
        for(int i =0; i<Math.min(expected.size(), res.size()); i++){
            assertThat("incorrect object", expected.get(i).getGuid(), is(res.get(i).getGuid()));
        }
    }

    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> AccessInfoTuple(
            final Long wsId,
            final String workspaceName,
            final String owner,
            final String time,
            final Long maxObjId,
            final String permUser,
            final String permGlobal,
            final String lockStatus,
            final Map metadata) {
        return new Tuple9<Long, String, String, String, Long, String,
                String, String, Map<String, String>>()
                .withE1(wsId)
                .withE2(workspaceName)
                .withE3(owner)
                .withE4(time)
                .withE5(maxObjId)
                .withE6(permUser)
                .withE7(permGlobal)
                .withE8(lockStatus)
                .withE9(metadata);
    }

    public static void compare(
            final Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>> got,
            final Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>> expected) {
        assertThat("incorrect workspace id", got.getE1(), is(expected.getE1()));
        assertThat("incorrect workspace name", got.getE2(), is(expected.getE2()));
        assertThat("incorrect owner", got.getE3(), is(expected.getE3()));
        assertThat("incorrect time", got.getE4(), is(expected.getE4()));
        assertThat("incorrect max object id", got.getE5(), is(expected.getE5()));
        assertThat("incorrect user permission", got.getE6(), is(expected.getE6()));
        assertThat("incorrect global permission", got.getE7(), is(expected.getE7()));
        assertThat("incorrect lock status", got.getE8(), is(expected.getE8()));
        assertThat("incorrect metadata", got.getE9(), is(expected.getE9()));
    }

    private SearchObjectsOutput getEmptySearchObjectsOutput(){
        return new SearchObjectsOutput()
                .withObjects(new ArrayList<>())
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withSearchTime(0L)
                .withTotal(0L)
                .withPagination(null)
                .withSortingRules(null);
    }

    public void compareAccessGroupInfo(final HashMap<Long, Tuple9> map1, final HashMap<Long, Tuple9> map2){
        assertThat("incorrect keys", map1.keySet(), is(map2.keySet()));
        for(Long key : map1.keySet()){
            compare(map1.get(key), map2.get(key));
        }
    }

}