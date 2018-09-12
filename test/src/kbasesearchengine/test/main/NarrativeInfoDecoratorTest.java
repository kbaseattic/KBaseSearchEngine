package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kbasesearchengine.*;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.NarrativeInfoDecorator;
import kbasesearchengine.main.NarrativeInfoProvider;
import kbasesearchengine.authorization.AuthInfoProvider;
import kbasesearchengine.main.NarrativeInfo;
import kbasesearchengine.main.AccessGroupNarrativeInfoProvider;

import kbasesearchengine.test.common.TestCommon;
import org.mockito.Mockito;
import us.kbase.common.service.JsonClientException;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;
import us.kbase.common.service.Tuple5;

public class NarrativeInfoDecoratorTest {

    @Test
    public void constructFail() {
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        failConstruct(null, nip, aip, new NullPointerException("SearchInterface"));
        failConstruct(search, null, aip, new NullPointerException("NarrativeInfoProvider"));
        failConstruct(search, nip, null, new NullPointerException("AuthInfoProvider"));
    }

    private void failConstruct(
            final SearchInterface search,
            final NarrativeInfoProvider nip,
            final AuthInfoProvider aip,
            final Exception expected) {
        try {
            new NarrativeInfoDecorator(search, nip, aip);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    /* Note that the implementation is the same for GetObjects and SearchObjects and so it
     * seems pointless to repeat the same tests for GetObjects. As such, only a very simple
     * test is run to ensure that objects are decorated.
     *
     * If the implementation of GetObjects changes be sure to update the tests.
     */

    @Test
    public void searchObjectsDecorateWithNoData() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(Collections.emptyList())
                .withAccessGroupNarrativeInfo(null));

        when(aip.findUserDisplayNames(set())).thenReturn(Collections.emptyMap());

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        assertThat("incorrect object data", res.getObjects(), is(Collections.emptyList()));
        assertThat("incorrect narr info", res.getAccessGroupNarrativeInfo(),
                is(Collections.emptyMap()));
    }

    @Test
    public void searchObjectsDecorateWithNullInfo() throws Exception {
        // also tests the case where a username in the workspace is, for some reason, not
        // found in the auth service results
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(AccessGroupNarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        final List<ObjectData> objectdata = new ArrayList<>(Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000")));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(null));

        // no narrative info at all
        when(nip.findNarrativeInfo(65L)).thenReturn(new NarrativeInfo(
                null, null, 1518126945000L, "owner1"));
        // only narrative id
        when(nip.findNarrativeInfo(1L)).thenReturn(new NarrativeInfo(
                null, null, 1518126957000L, "owner2"));

        // only narrative name
        when(nip.findNarrativeInfo(2L)).thenReturn(new NarrativeInfo(
                null, null, 1518126945678L, "owner3"));

        // full narrative info
        when(nip.findNarrativeInfo(42L)).thenReturn(new NarrativeInfo(
                "mylovelynarrative", 3L, 1518126950678L, "owner4"));

        when(aip.findUserDisplayNames(set("owner1", "owner2", "owner3", "owner4")))
                .thenReturn(ImmutableMap.of(
                        "owner1", "disp1",
                        "owner2", "disp2",
                        "owner4", "disp4")); // missing 3

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = ImmutableMap.of(
                65L, narrInfoTuple(null, null, 1518126945000L, "owner1", "disp1"),
                1L, narrInfoTuple(null, null, 1518126957000L, "owner2", "disp2"),
                2L, narrInfoTuple(null, null, 1518126945678L, "owner3", null),
                42L, narrInfoTuple("mylovelynarrative", 3L, 1518126950678L, "owner4", "disp4"));

        assertThat("incorrect object data", res.getObjects(), is(objectdata));

        compare(res.getAccessGroupNarrativeInfo(), expected);
    }

    @Test
    public void searchObjectsDecorateWithPreexistingInfo() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        final List<ObjectData> objectdata = new ArrayList<>(Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:2/345/1000")));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(ImmutableMap.of(
                        // expect overwrite
                        65L, narrInfoTuple("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld"),
                        32L, narrInfoTuple("narrname6", 2L, 20000L, "owner6", "Herbert K. Kornfeld"))));

        // no narrative info at all
        when(nip.findNarrativeInfo(65L)).thenReturn(new NarrativeInfo(
                null, null, 1518126945000L, "owner1"));

        when(nip.findNarrativeInfo(2L)).thenReturn(new NarrativeInfo(
                null, null, 1518126945678L, "owner3"));

        when(aip.findUserDisplayNames(set("owner1", "owner3"))).thenReturn(ImmutableMap.of(
                "owner1", "Gengulphus P. Twistleton", "owner3", "Fred"));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = ImmutableMap.of(
                65L, narrInfoTuple(null, null, 1518126945000L, "owner1", "Gengulphus P. Twistleton"),
                2L, narrInfoTuple(null, null, 1518126945678L, "owner3", "Fred"),
                32L, narrInfoTuple("narrname6", 2L, 20000L, "owner6", "Herbert K. Kornfeld"));

        assertThat("incorrect object data", res.getObjects(), is(objectdata));

        compare(res.getAccessGroupNarrativeInfo(), expected);
    }

    @Test
    public void failAddNarrativeInfo() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoProvider nip = new AccessGroupNarrativeInfoProvider(weh);
        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        when(weh.getWorkspaceInfo(65L)).thenThrow(new JsonClientException(
                "Failed retrieving workspace info: workspace is turned off"));
        when(weh.getWorkspaceInfo(42L)).thenReturn(wsTuple(
                42, "name4", "owner4", "2018-02-08T21:55:50.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "3", "narrative_nice_name", "mylovelynarrative")));
        when(aip.findUserDisplayNames(set("owner4"))).thenReturn(ImmutableMap.of(
                "owner4", "Fred"));

        final List<ObjectData> objectdata = new ArrayList<>(Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/1/1")));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");
        assertThat("Incorrect size", res.getAccessGroupNarrativeInfo().size(), is(1));
        assertNull("Null expected", res.getAccessGroupNarrativeInfo().get(65L));
        compare(res.getAccessGroupNarrativeInfo().get(42L),
                narrInfoTuple("mylovelynarrative", 3L, 1518126950678L, "owner4", "Fred"));
    }

    @Test
    public void failAddAuthInfo() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoProvider nip = new AccessGroupNarrativeInfoProvider(weh);
        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        when(weh.getWorkspaceInfo(35L)).thenThrow(new JsonClientException(
                "Failed retrieving workspace info: workspace is turned off"));
        when(weh.getWorkspaceInfo(12L)).thenReturn(wsTuple(
                12, "name4", "owner4", "2018-02-08T21:55:50.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "3", "narrative_nice_name", "mylovelynarrative")));
        when(aip.findUserDisplayNames(set("owner4"))).thenThrow(new Auth2Exception(
                "Failed retrieving auth info"));

        final List<ObjectData> objectdata = new ArrayList<>(Arrays.asList(
                new ObjectData().withGuid("WS:35/2/7"),
                new ObjectData().withGuid("WS:12/2/7")));

        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");
        assertThat("Incorrect size", res.getAccessGroupNarrativeInfo().size(), is(1));
        assertNull("Null expected", res.getAccessGroupNarrativeInfo().get(35L));
        compare(res.getAccessGroupNarrativeInfo().get(12L),
                narrInfoTuple("mylovelynarrative", 3L, 1518126950678L, "owner4", null));
    }

    @Test
    public void getObjectsSimpleTest() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final GetObjectsInput dummyInput = new GetObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

        final List<ObjectData> objectdata = new ArrayList<>(Arrays.asList(new ObjectData().withGuid("WS:42/7/21")));
        when(search.getObjects(dummyInput, "user")).thenReturn(new GetObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(null));

        // full narrative info
        when(nip.findNarrativeInfo(42L)).thenReturn(new NarrativeInfo(
                "mylovelynarrative", 3L, 1518126950678L, "owner4"));

        when(aip.findUserDisplayNames(set("owner4"))).thenReturn(ImmutableMap.of(
                "owner4", "foo"));

        final GetObjectsOutput res = nid.getObjects(dummyInput, "user");

        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = new HashMap<>();
        expected.put(42L, narrInfoTuple("mylovelynarrative", 3L, 1518126950678L, "owner4", "foo"));

        assertThat("incorrect object data", res.getObjects(), is(objectdata));

        compare(res.getAccessGroupNarrativeInfo(), expected);
    }

    @Test
    public void searchTypes() throws Exception {
        // this is just a passthrough
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated input and output classes have no hashcode or equals and
         * b) the method is just a straight pass through, we just use an identity match
         * for mockito to recognize the classes
         */

        final SearchTypesInput dummyInput = new SearchTypesInput();
        final SearchTypesOutput dummyOutput = new SearchTypesOutput();

        when(search.searchTypes(dummyInput, "user")).thenReturn(dummyOutput);

        assertThat("incorrect output", nid.searchTypes(dummyInput, "user"), is(dummyOutput));
    }

    @Test
    public void listTypes() throws Exception {
        // this is just a passthrough
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        /* since a) the generated output class has no hashcode or equals and
         * b) the method is just a straight pass through, we just use an identity match
         * for mockito to recognize the class
         */

        final TypeDescriptor dummyOutput = new TypeDescriptor();

        when(search.listTypes("type")).thenReturn(ImmutableMap.of("type", dummyOutput));

        assertThat("incorrect output", nid.listTypes("type"), is(ImmutableMap.of(
                "type", dummyOutput)));
    }

    @Test
    public void searchObjectsWithDeletedWs() throws Exception {
        final NarrativeInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
        final Pagination pag = new Pagination().withCount(3L).withStart(0L);
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L))
                .withPagination(pag);

        final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
        expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
        expectedObjs.add(new ObjectData().withGuid("WS:4/2/1").withCreator("user"));

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
        compare( res.getAccessGroupNarrativeInfo().get(4L), narrInfoTuple("test", 1L, 1L, "user", null));
        assertThat("removed guids not shown", res.getAdditionalProperties().get("removedGuids") == null, is(true));
        assertThat("incorrect key", res.getAccessGroupNarrativeInfo().get(3L) == null, is(true));
    }

    @Test
    public void searchObjectsWithDeletedWsWithShowingRemovedGuids() throws Exception {
        Map<String, String> env = TestCommon.getenv();

        try {
            final NarrativeInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
            final Pagination pag = new Pagination().withCount(3L).withStart(0L);
            final SearchObjectsInput dummyInput = new SearchObjectsInput()
                    .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L))
                    .withPagination(pag);

            final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
            expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
            expectedObjs.add(new ObjectData().withGuid("WS:4/2/1").withCreator("user"));

            env.put("KBASE_SEARCH_SHOW_REMOVED_GUIDS", "true");
            final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

            compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
            compare( res.getAccessGroupNarrativeInfo().get(4L), narrInfoTuple("test", 1L, 1L, "user", null));

            assertThat("removed guids shown", res.getAdditionalProperties().get("removedGuids"), is(Arrays.asList("WS:3/5/6")));
            assertThat("incorrect key", res.getAccessGroupNarrativeInfo().get(3L) == null, is(true));
        } finally {
            env.put("KBASE_SEARCH_SHOW_REMOVED_GUIDS", "");
        }
    }


    @Test
    public void getObjectsWithDeletedWsWithShowingRemovedGuids() throws Exception {
        Map<String, String> env = TestCommon.getenv();

        try{
            final NarrativeInfoDecorator nid = setUpSearchObjectsWithDeletedWs();
            final GetObjectsInput dummyInput = new GetObjectsInput()
                    .withPostProcessing(new PostProcessing().withAddNarrativeInfo(1L));

            final ArrayList<ObjectData> expectedObjs = new ArrayList<>();
            expectedObjs.add(new ObjectData().withGuid("WS:4/1/7").withCreator("user"));
            expectedObjs.add(new ObjectData().withGuid("WS:4/2/1").withCreator("user"));

            env.put("KBASE_SEARCH_SHOW_REMOVED_GUIDS", "true");
            final GetObjectsOutput res = nid.getObjects(dummyInput, "user");

            compareSearchObjectOutputRes(res.getObjects(), expectedObjs);
            compare( res.getAccessGroupNarrativeInfo().get(4L), narrInfoTuple("test", 1L, 1L, "user", null));

            assertThat("removed guids shown", res.getAdditionalProperties().get("removedGuids"), is(Arrays.asList("WS:3/5/6")));
            assertThat("incorrect key", res.getAccessGroupNarrativeInfo().get(3L) == null, is(true));
        }finally {
            env.put("KBASE_SEARCH_SHOW_REMOVED_GUIDS", "");
        }
    }

    public NarrativeInfoDecorator setUpSearchObjectsWithDeletedWs() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final NarrativeInfoProvider nip = mock(NarrativeInfoProvider.class);
        final AuthInfoProvider aip = mock(AuthInfoProvider.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, nip, aip);

        final Pagination pag = new Pagination().withCount(3L).withStart(0L);


        final ObjectData obj = new ObjectData().withGuid("WS:3/5/6").withCreator("user");
        final ObjectData obj2 = new ObjectData().withGuid("WS:4/1/7").withCreator("user");
        final ObjectData obj3 = new ObjectData().withGuid("WS:4/2/1").withCreator("user");


        final ArrayList<ObjectData> objs = new ArrayList<>();
        objs.add(obj);
        objs.add(obj2);
        objs.add(obj3);

        when(nip.findNarrativeInfo(3L)).thenReturn(null);
        when(nip.findNarrativeInfo(4L)).thenReturn(new NarrativeInfo("test", 1L, 1L, "user"));

        when(search.searchObjects(Mockito.any(SearchObjectsInput.class), eq("user"))).thenReturn(new SearchObjectsOutput()
                .withObjects(objs)
                .withTotal(10L)
                .withSearchTime(1L)
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withPagination(pag));


        when(search.getObjects(Mockito.any(GetObjectsInput.class), eq("user"))).thenReturn(new GetObjectsOutput()
                .withObjects(objs)
                .withAccessGroupNarrativeInfo(new HashMap<>())
                .withAccessGroupsInfo(new HashMap<>())
                .withObjectsInfo(new HashMap<>())
                .withSearchTime(1L));

        return nid;
    }

    public static void compare(
            final Map<Long, Tuple5<String, Long, Long, String, String>> got,
            final Map<Long, Tuple5<String, Long, Long, String, String>> expected) {
        assertThat("incorrect map keys", got.keySet(), is(expected.keySet()));
        for (final Long key: got.keySet()) {
            compare(got.get(key), expected.get(key));
        }
    }

    public static void compareSearchObjectOutputRes(List<ObjectData> expected, List<ObjectData> res ){
        assertThat("incorrect number of results", expected.size(), is(res.size()));
        for(int i =0; i<Math.min(expected.size(), res.size()); i++){
            if(res.get(i) == null){
                assertThat("incorrect object", expected.get(i) == null, is(true));
            }else{
                assertThat("incorrect object", expected.get(i).getGuid(), is(res.get(i).getGuid()));
            }
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

}