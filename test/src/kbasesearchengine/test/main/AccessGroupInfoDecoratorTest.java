package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.*;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import kbasesearchengine.*;
import kbasesearchengine.events.handler.CloneableWorkspaceClient;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.main.SearchInterface;
import kbasesearchengine.main.AccessGroupInfoDecorator;
import kbasesearchengine.main.AccessGroupInfoProvider;
import kbasesearchengine.main.ObjectInfoProvider;
import kbasesearchengine.main.AccessGroupNarrativeInfoProvider;
import us.kbase.common.service.UObject;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.Tuple11;

import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.JsonClientException;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.GetObjectInfo3Results;

public class AccessGroupInfoDecoratorTest {

    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_v1 = wsTuple(65, "myws65_v1", "owner65", "date65", 32, "a",
            "r", "unlocked",Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65 = wsTuple(65, "myws65", "owner65", "date65", 32, "a",
            "r", "unlocked",Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_42 = wsTuple(42, "myws42", "owner42", "date42", 32, "a",
            "r", "unlocked",Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_1 = wsTuple(1, "myws1", "owner1", "date1", 32, "a",
            "r", "unlocked",Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_2 = wsTuple(2, "myws2", "owner2", "date2", 32, "a",
            "r", "unlocked",Collections.emptyMap());

    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_65_2_7 =
            objTuple(2L, "objName2", "sometype", "date2", 7L,"copier2",
                    65L, "wsname65", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_65_2_7_v1 =
            objTuple(2L, "objName2", "sometype", "date2", 7L,"copier2",
                    65L, "wsname65_v1", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_42_7_21 =
            objTuple(7L, "objName7", "sometype", "date7", 21L, "copier7",
                    42L, "wsname42", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_1_61_1 =
            objTuple(61L, "objName61", "sometype", "date61", 1L, "copier61",
                    1L, "wsname1", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_2_345_1000 =
            objTuple(345L, "objName345", "sometype", "date345", 1L, "copier345",
                    2L, "wsname2", "checksum", 44, Collections.emptyMap());

    @Test
    public void constructFail() {
        final SearchInterface search = mock(SearchInterface.class);
        final AccessGroupInfoProvider agip = mock(AccessGroupInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        failConstruct(null, agip, oip, new NullPointerException("searchInterface"));
        failConstruct(search, null, oip, new NullPointerException("accessGroupInfoProvider"));
        failConstruct(search, agip, null, new NullPointerException("objectInfoProvider"));
    }

    private void failConstruct(
            final SearchInterface search,
            final AccessGroupInfoProvider agip,
            final ObjectInfoProvider oip,
            final Exception expected) {
        try {
            new AccessGroupInfoDecorator(search, agip, oip);
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
        final AccessGroupInfoProvider agip = mock(AccessGroupInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoDecorator nid = new AccessGroupInfoDecorator(search, agip, oip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */
        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L));

        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(Collections.emptyList())
                .withAccessGroupsInfo(null)
                .withObjectsInfo(null));

        when(oip.getObjectsInfo(set())).thenReturn(Collections.emptyMap());

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        assertThat("incorrect object data", res.getObjects(), is(Collections.emptyList()));
        assertThat("incorrect access groups info", res.getAccessGroupsInfo(),
                is(Collections.emptyMap()));
        assertThat("incorrect objects info", res.getObjectsInfo(),
                is(Collections.emptyMap()));
    }

    @Test
    public void searchObjectsDecorateWithNullInfo() throws Exception {

        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoProvider agip = new AccessGroupNarrativeInfoProvider(weh);
        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, agip, oip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L));

        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupsInfo(null)
                .withObjectsInfo(null));

        when(weh.getWorkspaceInfo(65L)).thenReturn(wsTuple_65);
        when(weh.getWorkspaceInfo(1L)).thenReturn(wsTuple_1);
        when(weh.getWorkspaceInfo(2L)).thenReturn(wsTuple_2);
        when(weh.getWorkspaceInfo(42L)).thenReturn(wsTuple_42);

        when(oip.getObjectsInfo(set("65/2/7", "42/7/21", "1/61/1", "2/345/1000")))
                .thenReturn(ImmutableMap.of(
                        "65/2/7", objTuple_65_2_7,
                        "42/7/21", objTuple_42_7_21,
                        "2/345/1000", objTuple_2_345_1000,
                        "1/61/1", objTuple_1_61_1));

        final SearchObjectsOutput searchResults = agid.searchObjects(dummyInput, "user");

        // verify the values in workspacesInfo map
        compareWsInfo(searchResults.getAccessGroupsInfo().get(65L), wsTuple_65);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(42L), wsTuple_42);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(2L), wsTuple_2);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(1L), wsTuple_1);
        assertFalse("Data source FS is in access groups info", searchResults.getAccessGroupsInfo().containsKey(6L));

        // verify the values in objectsInfo map
        compareObjInfo(searchResults.getObjectsInfo().get("1/61/1"), objTuple_1_61_1);
        compareObjInfo(searchResults.getObjectsInfo().get("2/345/1000"), objTuple_2_345_1000);
        compareObjInfo(searchResults.getObjectsInfo().get("65/2/7"), objTuple_65_2_7);
        compareObjInfo(searchResults.getObjectsInfo().get("42/7/21"), objTuple_42_7_21);
        assertFalse("Data source FS is in objects info", searchResults.getObjectsInfo().containsKey("6/22/3"));
    }

    @Test
    public void searchObjectsDecorateWithPreexistingInfo() throws Exception {

        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoProvider agip = new AccessGroupNarrativeInfoProvider(weh);
        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, agip, oip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L));

        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupsInfo(ImmutableMap.of(
                        // expect overwrite
                        65L, wsTuple_65_v1,
                        42L, wsTuple_42))
                .withObjectsInfo(ImmutableMap.of(
                        // expect overwrite
                        "65/2/7", objTuple_65_2_7_v1,
                        "42/7/21", objTuple_42_7_21)));

        when(weh.getWorkspaceInfo(65L)).thenReturn(wsTuple_65);
        when(weh.getWorkspaceInfo(2L)).thenReturn(wsTuple_2);

        when(oip.getObjectsInfo(set("65/2/7", "2/345/1000")))
                .thenReturn(ImmutableMap.of(
                        "65/2/7", objTuple_65_2_7,
                        "2/345/1000", objTuple_2_345_1000));

        final SearchObjectsOutput searchResults = agid.searchObjects(dummyInput, "user");

        // verify the values in workspacesInfo map
        compareWsInfo(searchResults.getAccessGroupsInfo().get(65L), wsTuple_65);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(42L), wsTuple_42);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(2L), wsTuple_2);
        // verify the values in objectsInfo map
        compareObjInfo(searchResults.getObjectsInfo().get("2/345/1000"), objTuple_2_345_1000);
        compareObjInfo(searchResults.getObjectsInfo().get("65/2/7"), objTuple_65_2_7);
        compareObjInfo(searchResults.getObjectsInfo().get("42/7/21"), objTuple_42_7_21);
    }

    @Test
    public void failSearchObjectsDecorateWithNullInfo() throws Exception {
        // also tests the case where a username in the workspace is, for some reason, not
        // found in the auth service results
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoProvider agip = new AccessGroupNarrativeInfoProvider(weh);
        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, agip, oip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L));

        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(null));

        when(weh.getWorkspaceInfo(65L)).thenReturn(wsTuple_65);
        when(weh.getWorkspaceInfo(1L)).thenReturn(wsTuple_1);
        when(weh.getWorkspaceInfo(2L)).thenReturn(wsTuple_2);
        when(weh.getWorkspaceInfo(42L)).thenThrow(new JsonClientException(
                "Failed retrieving workspace info: workspace is turned off"));

        Map<String, Tuple11<Long, String, String, String, Long, String,
                Long, String, String, Long, Map<String, String>>> objInfoMap = new HashMap<>();
        objInfoMap.put("65/2/7", objTuple_65_2_7);
        objInfoMap.put("42/7/21", null);
        objInfoMap.put("2/345/1000", objTuple_2_345_1000);
        objInfoMap.put("1/61/1", objTuple_1_61_1);

        when(oip.getObjectsInfo(set("65/2/7", "42/7/21", "1/61/1", "2/345/1000")))
                .thenReturn(objInfoMap);

        final SearchObjectsOutput searchResults = agid.searchObjects(dummyInput, "user");

        // verify the values in workspacesInfo map
        compareWsInfo(searchResults.getAccessGroupsInfo().get(65L), wsTuple_65);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(2L), wsTuple_2);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(1L), wsTuple_1);
        assertNull("Access Group info not null", searchResults.getAccessGroupsInfo().get(42L));
        assertFalse("Data source FS is in access groups info", searchResults.getAccessGroupsInfo().containsKey(6L));

        // verify the values in objectsInfo map
        compareObjInfo(searchResults.getObjectsInfo().get("1/61/1"), objTuple_1_61_1);
        compareObjInfo(searchResults.getObjectsInfo().get("2/345/1000"), objTuple_2_345_1000);
        compareObjInfo(searchResults.getObjectsInfo().get("65/2/7"), objTuple_65_2_7);
        assertNull("Object info not null", searchResults.getObjectsInfo().get("42/7/21"));
        assertFalse("Data source FS is in objects info", searchResults.getObjectsInfo().containsKey("6/22/3"));
    }

    @Test
    public void getObjectsSimpleTest() throws Exception {
        final SearchInterface search = mock(SearchInterface.class);
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoProvider agip = new AccessGroupNarrativeInfoProvider(weh);
        final AccessGroupInfoDecorator agid = new AccessGroupInfoDecorator(search, agip, oip);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final GetObjectsInput dummyInput = new GetObjectsInput()
                .withPostProcessing(new PostProcessing().withAddAccessGroupInfo(1L));

        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.getObjects(dummyInput, "user")).thenReturn(new GetObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupsInfo(null)
                .withObjectsInfo(null));

        when(weh.getWorkspaceInfo(65L)).thenReturn(wsTuple_65);
        when(weh.getWorkspaceInfo(1L)).thenReturn(wsTuple_1);
        when(weh.getWorkspaceInfo(2L)).thenReturn(wsTuple_2);
        when(weh.getWorkspaceInfo(42L)).thenReturn(wsTuple_42);

        when(oip.getObjectsInfo(set("65/2/7", "42/7/21", "1/61/1", "2/345/1000")))
                .thenReturn(ImmutableMap.of(
                        "65/2/7", objTuple_65_2_7,
                        "42/7/21", objTuple_42_7_21,
                        "2/345/1000", objTuple_2_345_1000,
                        "1/61/1", objTuple_1_61_1));

        final GetObjectsOutput searchResults = agid.getObjects(dummyInput, "user");

        // verify the values in workspacesInfo map
        compareWsInfo(searchResults.getAccessGroupsInfo().get(65L), wsTuple_65);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(42L), wsTuple_42);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(2L), wsTuple_2);
        compareWsInfo(searchResults.getAccessGroupsInfo().get(1L), wsTuple_1);
        assertFalse("Data source FS is in access groups info", searchResults.getAccessGroupsInfo().containsKey(6L));

        // verify the values in objectsInfo map
        compareObjInfo(searchResults.getObjectsInfo().get("1/61/1"), objTuple_1_61_1);
        compareObjInfo(searchResults.getObjectsInfo().get("2/345/1000"), objTuple_2_345_1000);
        compareObjInfo(searchResults.getObjectsInfo().get("65/2/7"), objTuple_65_2_7);
        compareObjInfo(searchResults.getObjectsInfo().get("42/7/21"), objTuple_42_7_21);
        assertFalse("Data source FS is in objects info", searchResults.getObjectsInfo().containsKey("6/22/3"));
      }

    @Test
    public void searchTypes() throws Exception {
        // this is just a passthrough
        final SearchInterface search = mock(SearchInterface.class);
        final AccessGroupInfoProvider nip = mock(AccessGroupInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoDecorator nid = new AccessGroupInfoDecorator(search, nip, oip);

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
        final AccessGroupInfoProvider nip = mock(AccessGroupInfoProvider.class);
        final ObjectInfoProvider oip = mock(ObjectInfoProvider.class);

        final AccessGroupInfoDecorator nid = new AccessGroupInfoDecorator(search, nip, oip);

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
    public void getObjectInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);

        final String objName1 = "ObjectName1";

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList =
                new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple_1_61_1);

        List<List<String>> pathList = new ArrayList<>();
        List<String> path1 = new ArrayList<>();
        path1.add("1/61/1");
        pathList.add(path1);

        when(wscli.administer(any()))
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList).withPaths(pathList)));

        compareObjInfo(oip.getObjectInfo("1/61/1"), objTuple_1_61_1);
    }

    @Test
    public void failGetObjectInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);

        final String objName1 = "ObjectName1";

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList =
                new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(null);

        List<List<String>> pathList = new ArrayList<>();
        pathList.add(null);

        when(wscli.administer(any()))
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList).withPaths(pathList)));

        assertNull("Obj Info not null", oip.getObjectInfo("1/61/1"));
    }

    @Test
    public void getObjectsInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList =
                new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple_65_2_7);
        objList.add(objTuple_42_7_21);
        List<List<String>> pathList = new ArrayList<>();
        List<String> path1 = new ArrayList<>();
        path1.add("65/2/7");
        List<String> path2 = new ArrayList<>();
        path2.add("42/7/21");
        pathList.add(path1);
        pathList.add(path2);

        when(wscli.administer(any()))
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList).withPaths(pathList)));

        compareObjInfoMap(oip.getObjectsInfo(set("65/2/7", "42/7/21")),
                ImmutableMap.of(
                        "65/2/7", objTuple_65_2_7,
                        "42/7/21", objTuple_42_7_21));
    }

    @Test
    public void failGetObjectsInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList =
                new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple_65_2_7);
        objList.add(null);
        objList.add(objTuple_42_7_21);
        List<List<String>> pathList = new ArrayList<>();
        List<String> path1 = new ArrayList<>();
        path1.add("65/2/7");
        List<String> path2 = new ArrayList<>();
        path2.add("42/7/21");
        pathList.add(path1);
        pathList.add(null);
        pathList.add(path2);

        when(wscli.administer(any()))
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList).withPaths(pathList)));

        Map<String, Tuple11<Long, String, String, String, Long, String,
                Long, String, String, Long, Map<String, String>>> retVal = oip.getObjectsInfo(set("65/2/7", "1/2/1", "42/7/21"));

        compareObjInfo(retVal.get("42/7/21"), objTuple_42_7_21);
        compareObjInfo(retVal.get("65/2/7"), objTuple_65_2_7);
        assertNull("Obj Info not null", retVal.get("1/2/1"));
    }
}