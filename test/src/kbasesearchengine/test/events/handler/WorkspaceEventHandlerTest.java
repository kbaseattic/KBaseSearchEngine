package kbasesearchengine.test.events.handler;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.system.StorageObjectType;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.UnprocessableEventIndexingException;
import kbasesearchengine.events.handler.CloneableWorkspaceClient;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UObject;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.workspace.GetObjectInfo3Results;
import us.kbase.workspace.GetObjects2Params;
import us.kbase.workspace.GetObjects2Results;
import us.kbase.workspace.ObjectData;
import us.kbase.workspace.ObjectSpecification;
import us.kbase.workspace.ProvenanceAction;
import us.kbase.workspace.SubAction;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.WorkspaceIdentity;

public class WorkspaceEventHandlerTest {
    
    // these are strictly unit tests.
    
    //TODO TEST add more tests. coverage is no where near 100%.
    
    @Test
    public void getStorageCode() {
        final CloneableWorkspaceClient cli = mock(CloneableWorkspaceClient.class);
        
        assertThat("incorrect storage code", new WorkspaceEventHandler(cli).getStorageCode(),
                is("WS"));
    }
    
    @Test
    public void constructFail() {
        try {
            new WorkspaceEventHandler(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(
                    got, new NullPointerException("clonableWorkspaceClient"));
        }
    }
    
    @Test
    public void parseDate() {
        assertThat("incorrect epoch",
                WorkspaceEventHandler.parseDateToEpochMillis("2018-02-08T21:55:45Z"),
                is(1518126945000L));
        assertThat("incorrect epoch",
                WorkspaceEventHandler.parseDateToEpochMillis("2018-02-08T21:55:45.123Z"),
                is(1518126945123L));
        assertThat("incorrect epoch",
                WorkspaceEventHandler.parseDateToEpochMillis("2018-02-08T21:55:45+00:00"),
                is(1518126945000L));
    }
    
    @Test
    public void getWorkspaceInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);
        
        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(34))))
                .thenReturn(new UObject(wsTuple(64, "myws", "owner", "date", 32, "a",
                            "r", "unlocked", Collections.emptyMap())));
        
        final Tuple9<Long, String, String, String, Long, String, String, String,
                Map<String, String>> res = weh.getWorkspaceInfo(34);
        
        compare(res, wsTuple(64, "myws", "owner", "date", 32, "a",
                "r", "unlocked", Collections.emptyMap()));
    }
    
    private void compare(
            final Tuple9<Long, String, String, String, Long, String, String, String,
                    Map<String, String>> got,
            final Tuple9<Long, String, String, String, Long, String, String, String,
                    Map<String, String>> expected) {
        assertThat("incorrect ws id", got.getE1(), is(expected.getE1()));
        assertThat("incorrect ws name", got.getE2(), is(expected.getE2()));
        assertThat("incorrect ws owner", got.getE3(), is(expected.getE3()));
        assertThat("incorrect ws date", got.getE4(), is(expected.getE4()));
        assertThat("incorrect ws obj count", got.getE5(), is(expected.getE5()));
        assertThat("incorrect ws user perm", got.getE6(), is(expected.getE6()));
        assertThat("incorrect ws global perm", got.getE7(), is(expected.getE7()));
        assertThat("incorrect ws lock state", got.getE8(), is(expected.getE8()));
        assertThat("incorrect ws meta", got.getE9(), is(expected.getE9()));
    }

    private class AdminGetObjectsAnswerMatcher implements ArgumentMatcher<UObject> {

        final String ref;
        
        public AdminGetObjectsAnswerMatcher(final String ref) {
            this.ref = ref;
        }
        
        @Override
        public boolean matches(final UObject command) {
            // the fact that kb-sdk doesn't compile in equals & hashcode is infuriating
            boolean matches = true;
            @SuppressWarnings("unchecked")
            final Map<String, Object> cmd = command.asClassInstance(Map.class);
            matches = matches && "getObjects".equals(cmd.get("command"));
            
            final GetObjects2Params p = UObject.transformObjectToObject(
                    cmd.get("params"), GetObjects2Params.class);
            matches = matches && p.getIgnoreErrors() == null;
            matches = matches && p.getNoData() == null;
            
            final List<ObjectSpecification> objslist = p.getObjects();
            matches = matches && objslist.size() == 1;
            
            final ObjectSpecification spec = objslist.get(0);
            matches = matches && spec.getFindReferencePath() == null;
            matches = matches && spec.getIncluded() == null;
            matches = matches && spec.getName() == null;
            matches = matches && spec.getObjid() == null;
            matches = matches && spec.getObjPath() == null;
            matches = matches && spec.getObjRefPath() == null;
            matches = matches && spec.getStrictArrays() == null;
            matches = matches && spec.getStrictMaps() == null;
            matches = matches && spec.getToObjPath() == null;
            matches = matches && spec.getToObjRefPath() == null;
            matches = matches && spec.getVer() == null;
            matches = matches && spec.getWorkspace() == null;
            matches = matches && spec.getWsid() == null;
            matches = matches && ref.equals(spec.getRef());
            
            return matches;
        }
    }
    
    private class AdminGetWSInfoAnswerMatcher implements ArgumentMatcher<UObject> {

        final long id;
        
        public AdminGetWSInfoAnswerMatcher(final long id) {
            this.id = id;
        }
        
        @Override
        public boolean matches(final UObject command) {
            // the fact that kb-sdk doesn't compile in equals & hashcode is infuriating
            boolean matches = true;
            @SuppressWarnings("unchecked")
            final Map<String, Object> cmd = command.asClassInstance(Map.class);
            matches = matches && "getWorkspaceInfo".equals(cmd.get("command"));

            final WorkspaceIdentity wi = UObject.transformObjectToObject(
                    cmd.get("params"), WorkspaceIdentity.class);
            matches = matches && wi.getId() == id;
            matches = matches && wi.getWorkspace() == null;

            return matches;
        }
    }
    
    @Test
    public void loadNoPathMinimal() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        doReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                new ObjectData()
                        .withData(new UObject(ImmutableMap.of("genome", "data")))
                        .withProvenance(Collections.emptyList())
                        .withCreator("creator")
                        .withCopySourceInaccessible(0L)
                        .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                1, "wsname", "checksum", 44, Collections.emptyMap()))))))
                .when(cloned).administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3")));
        
        doReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                "unlocked", Collections.emptyMap())))
                .when(wscli).administer(argThat(new AdminGetWSInfoAnswerMatcher(1)));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(new GUID("WS:1/2/3"), Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableMD5("checksum")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }

    @Test
    public void loadWithPathMaximalCopyRef() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        when(cloned.administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3;4/5/6"))))
                .thenReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                        new ObjectData()
                                .withData(new UObject(ImmutableMap.of("genome", "data")))
                                .withProvenance(Arrays.asList(new ProvenanceAction()
                                        .withMethod("meth")
                                        .withService("serv")
                                        .withServiceVer("sver")
                                        .withSubactions(Arrays.asList(
                                                new SubAction()
                                                        .withCommit("commit")
                                                        .withName("serv.meth"),
                                                // this one should be ignored
                                                new SubAction()
                                                        .withCommit("commit2")
                                                        .withName("serv.meth2")))))
                                .withCreator("creator")
                                .withCopied("7/8/9")
                                .withCopySourceInaccessible(0L)
                                .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                        1, "wsname", "checksum", 44, Collections.emptyMap()))))));
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(Arrays.asList(new GUID("WS:1/2/3"), new GUID("WS:4/5/6")),
                        Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableCopier("copier")
                .withNullableMD5("checksum")
                .withNullableCommitHash("commit")
                .withNullableMethod("meth")
                .withNullableModule("serv")
                .withNullableVersion("sver")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }
    
    @Test
    public void loadWith1ItemPathEmptySubActionsInaccessibleCopy() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        when(cloned.administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3"))))
                .thenReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                        new ObjectData()
                                .withData(new UObject(ImmutableMap.of("genome", "data")))
                                .withProvenance(Arrays.asList(new ProvenanceAction()
                                        .withMethod("meth")
                                        .withService("serv")
                                        .withServiceVer("sver")
                                        .withSubactions(Collections.emptyList())))
                                .withCreator("creator")
                                .withCopySourceInaccessible(1L)
                                .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                        1, "wsname", "checksum", 44, Collections.emptyMap()))))));
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(Arrays.asList(new GUID("WS:1/2/3")),
                        Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableCopier("copier")
                .withNullableMD5("checksum")
                .withNullableMethod("meth")
                .withNullableModule("serv")
                .withNullableVersion("sver")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }
    
    @Test
    public void loadWithNullSubActionsAndTags() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        when(cloned.administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3"))))
                .thenReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                        new ObjectData()
                                .withData(new UObject(ImmutableMap.of("genome", "data")))
                                .withProvenance(Arrays.asList(new ProvenanceAction()
                                        .withMethod("meth")
                                        .withService("serv")
                                        .withServiceVer("sver")
                                        .withSubactions(null)))
                                .withCreator("creator")
                                .withCopySourceInaccessible(0L)
                                .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                        1, "wsname", "checksum", 44, Collections.emptyMap()))))));
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked",
                        ImmutableMap.of("searchtags", "foo, \t  \n  ,    bar  \t  , baz"))));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(Arrays.asList(new GUID("WS:1/2/3")),
                        Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableMD5("checksum")
                .withNullableMethod("meth")
                .withNullableModule("serv")
                .withNullableVersion("sver")
                .withSourceTag("foo")
                .withSourceTag("bar")
                .withSourceTag("baz")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }
    
    @Test
    public void loadWithNoProvMethod() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        when(cloned.administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3"))))
                .thenReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                        new ObjectData()
                                .withData(new UObject(ImmutableMap.of("genome", "data")))
                                .withProvenance(Arrays.asList(new ProvenanceAction()
                                        .withService("serv")
                                        .withServiceVer("sver")
                                        .withSubactions(Arrays.asList(
                                                new SubAction()
                                                .withCommit("commit")
                                                .withName("serv.meth")))))
                                .withCreator("creator")
                                .withCopySourceInaccessible(0L)
                                .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                        1, "wsname", "checksum", 44, Collections.emptyMap()))))));
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(Arrays.asList(new GUID("WS:1/2/3")),
                        Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableMD5("checksum")
                .withNullableModule("serv")
                .withNullableVersion("sver")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }
    
    @Test
    public void loadWithNoProvService() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        when(cloned.administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3"))))
                .thenReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                        new ObjectData()
                                .withData(new UObject(ImmutableMap.of("genome", "data")))
                                .withProvenance(Arrays.asList(new ProvenanceAction()
                                        .withMethod("meth")
                                        .withServiceVer("sver")
                                        .withSubactions(Arrays.asList(
                                                new SubAction()
                                                .withCommit("commit")
                                                .withName("serv.meth")))))
                                .withCreator("creator")
                                .withCopySourceInaccessible(0L)
                                .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                        1, "wsname", "checksum", 44, Collections.emptyMap()))))));
        
        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())));
        
        final SourceData sd = new WorkspaceEventHandler(clonecli)
                .load(Arrays.asList(new GUID("WS:1/2/3")),
                        Paths.get("somefile"));
        
        final SourceData expected = SourceData.getBuilder(
                new UObject(ImmutableMap.of("genome", "data")), "objname", "creator")
                .withNullableMD5("checksum")
                .withNullableMethod("meth")
                .withNullableVersion("sver")
                .build();
        
        compare(sd, expected);
        
        verify(cloned).setStreamingModeOn(true);
        verify(cloned)._setFileForNextRpcResponse(new File("somefile"));
    }

    private void compare(final SourceData sd, final SourceData expected) {
        assertThat("incorrect data", sd.getData().asClassInstance(Map.class),
                is(expected.getData().asClassInstance(Map.class)));
        assertThat("incorrect name", sd.getName(), is(expected.getName()));
        assertThat("incorrect creator", sd.getCreator(), is(expected.getCreator()));
        assertThat("incorrect copier", sd.getCopier(), is(expected.getCopier()));
        assertThat("incorrect commit", sd.getCommitHash(), is(expected.getCommitHash()));
        assertThat("incorrect method", sd.getMethod(), is(expected.getMethod()));
        assertThat("incorrect module", sd.getModule(), is(expected.getModule()));
        assertThat("incorrect version", sd.getVersion(), is(expected.getVersion()));
        assertThat("incorrect md5", sd.getMD5(), is(expected.getMD5()));
        assertThat("incorrect tags", sd.getSourceTags(), is(expected.getSourceTags()));
    }
    
    public static Tuple9<Long, String, String, String, Long, String, String, String,
        Map<String, String>> wsTuple(
            final long wsid,
            final String wsname,
            final String owner,
            final String date,
            final long objcount,
            final String userperm,
            final String publicread,
            final String locked,
            final Map<String, String> meta) {
        return new Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>>()
                .withE1(wsid)
                .withE2(wsname)
                .withE3(owner)
                .withE4(date)
                .withE5(objcount)
                .withE6(userperm)
                .withE7(publicread)
                .withE8(locked)
                .withE9(meta);
    }

    private Tuple11<Long, String, String, String, Long, String, Long, String, String, Long,
        Map<String, String>> objTuple(
            final long objid,
            final String name,
            final String type,
            final String date,
            final long version,
            final String user,
            final long wsid,
            final String workspace,
            final String chksum,
            final long size,
            final Map<String, String> meta) {
        return new Tuple11<Long, String, String, String, Long, String, Long, String, String, Long,
            Map<String, String>>()
                .withE1(objid)
                .withE2(name)
                .withE3(type)
                .withE4(date)
                .withE5(version)
                .withE6(user)
                .withE7(wsid)
                .withE8(workspace)
                .withE9(chksum)
                .withE10(size)
                .withE11(meta);
    }

    @Test
    public void loadFailNulls() {
        final Path f = Paths.get("foo");
        final GUID g = new GUID("WS:1/2/3");
        failLoadNulls((GUID) null, f, new NullPointerException("guid"));
        failLoadNulls(g, null, new NullPointerException("file"));
        
        failLoadNulls((List<GUID>) null, f, new NullPointerException("guids"));
        failLoadNulls(Arrays.asList(g), null, new NullPointerException("file"));
        failLoadNulls(Arrays.asList(g, null), null,
                new NullPointerException("null item in guids"));
    }
    
    private void failLoadNulls(final GUID guid, final Path file, final Exception expected) {
        try {
            new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(
                    mock(WorkspaceClient.class)))
                    .load(guid, file);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    private void failLoadNulls(final List<GUID> guids, final Path file, final Exception expected) {
        try {
            new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(
                    mock(WorkspaceClient.class)))
                    .load(guids, file);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    @Test
    public void loadFailBadStorageCode() {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(wscli);
        try {
            new WorkspaceEventHandler(clonecli)
                    .load(Arrays.asList(new GUID("FS:1/2/3/")), Paths.get("foo"));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got,
                    new IllegalArgumentException("GUID FS:1/2/3 is not a workspace object"));
        }
    }
    
    @Test
    public void loadFailWSGetObjExceptions() throws Exception {
        failLoadWSGetObjException(new ConnectException("hot damn"),
                new FatalRetriableIndexingException(ErrorType.OTHER, "hot damn"));
        
        failLoadWSGetObjException(new IOException("pump yer brakes, kid"),
                new RetriableIndexingException(ErrorType.OTHER, "pump yer brakes, kid"));
        
        failLoadWSGetObjException(new UnauthorizedException("dvd commentary"),
                new FatalIndexingException(ErrorType.OTHER, "dvd commentary"));
        
        failLoadWSGetObjException(new JsonClientException(null),
                new UnprocessableEventIndexingException(
                        ErrorType.OTHER, "Null error message from workspace server"));
        
        failLoadWSGetObjException(new JsonClientException("Object Whatever has Been Deleted"),
                new UnprocessableEventIndexingException(ErrorType.DELETED, 
                        "Object Whatever has Been Deleted"));
        failLoadWSGetObjException(new JsonClientException("Object Whatever is Deleted"),
                new UnprocessableEventIndexingException(ErrorType.DELETED, 
                        "Object Whatever is Deleted"));
        
        failLoadWSGetObjException(new JsonClientException("Couldn't Login"),
                new FatalIndexingException(ErrorType.OTHER, 
                        "Workspace credentials are invalid: Couldn't Login"));
        
        failLoadWSGetObjException(new JsonClientException("Did not start Up Properly"),
                new FatalIndexingException(ErrorType.OTHER, 
                        "Fatal error returned from workspace: Did not start Up Properly"));
        
        failLoadWSGetObjException(new JsonClientException("That man's a national treasure"),
                new UnprocessableEventIndexingException(ErrorType.OTHER, 
                        "Unrecoverable error from workspace on fetching object: " +
                        "That man's a national treasure"));
    }
    
    @Test
    public void loadFailWSGetWSInfoExceptions() throws Exception {
        failLoadWSGetWSInfoException(new ConnectException("hot damn"),
                new FatalRetriableIndexingException(ErrorType.OTHER, "hot damn"));
        
        failLoadWSGetWSInfoException(new IOException("pump yer brakes, kid"),
                new RetriableIndexingException(ErrorType.OTHER, "pump yer brakes, kid"));
        
        failLoadWSGetWSInfoException(new UnauthorizedException("dvd commentary"),
                new FatalIndexingException(ErrorType.OTHER, "dvd commentary"));
        
        failLoadWSGetWSInfoException(new JsonClientException(null),
                new UnprocessableEventIndexingException(
                        ErrorType.OTHER, "Null error message from workspace server"));
        
        failLoadWSGetWSInfoException(new JsonClientException("Object Whatever has Been Deleted"),
                new UnprocessableEventIndexingException(ErrorType.DELETED, 
                        "Object Whatever has Been Deleted"));
        failLoadWSGetWSInfoException(new JsonClientException("Object Whatever is Deleted"),
                new UnprocessableEventIndexingException(ErrorType.DELETED, 
                        "Object Whatever is Deleted"));
        
        failLoadWSGetWSInfoException(new JsonClientException("Couldn't Login"),
                new FatalIndexingException(
                        ErrorType.OTHER, "Workspace credentials are invalid: Couldn't Login"));
        
        failLoadWSGetWSInfoException(new JsonClientException("Did not start Up Properly"),
                new FatalIndexingException(ErrorType.OTHER, 
                        "Fatal error returned from workspace: Did not start Up Properly"));
        
        failLoadWSGetWSInfoException(new JsonClientException("That man's a national treasure"),
                new UnprocessableEventIndexingException(ErrorType.OTHER, 
                        "Unrecoverable error from workspace on fetching object: " +
                        "That man's a national treasure"));
    }
    
    private void failLoadWSGetObjException(final Exception toThrow, final Exception expected)
            throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(wscli);
        
        when(wscli.administer(any())).thenThrow(toThrow);
        
        try {
            new WorkspaceEventHandler(clonecli).load(new GUID("WS:1/2/3"), Paths.get("foo"));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failLoadWSGetWSInfoException(final Exception toThrow, final Exception expected)
            throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);
        
        doReturn(new UObject(new GetObjects2Results().withData(Arrays.asList(
                new ObjectData()
                        .withData(new UObject(ImmutableMap.of("genome", "data")))
                        .withProvenance(Collections.emptyList())
                        .withCreator("creator")
                        .withCopySourceInaccessible(0L)
                        .withInfo(objTuple(2, "objname", "sometype", "date", 3, "copier",
                                1, "wsname", "checksum", 44, Collections.emptyMap()))))))
                .when(cloned).administer(argThat(new AdminGetObjectsAnswerMatcher("1/2/3")));
        
        when(wscli.administer(any())).thenThrow(toThrow);
        
        try {
            new WorkspaceEventHandler(clonecli).load(new GUID("WS:1/2/3"), Paths.get("foo"));
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    @Test
    public void updateEventAccessGroupEvent() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                "WS",
                           Instant.ofEpochMilli(20000),
                           StatusEventType.PUBLISH_ACCESS_GROUP)
                           .withNullableAccessGroupID(1)
                           .build();

        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenReturn(new UObject(wsTuple(1, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        StatusEvent updatedEvent = weh.updateEvent(event);

        // access group events are not updated, so we expect to get back the original event
        Assert.assertEquals("expected the same event object", event, updatedEvent);
    }

    @Test
    public void updateEventWorkspacePermanentlyDeleted() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        when(wscli.administer(argThat(new AdminGetWSInfoAnswerMatcher(1))))
                .thenThrow(new JsonClientException("No workspace with id 1 exists!"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        StatusEvent updatedEvent = weh.updateEvent(event);

        // since workspace does not exist, we expect to get back an updated event
        Assert.assertNotSame("expected different event object", event, updatedEvent);
        Assert.assertEquals(updatedEvent.getEventType(), StatusEventType.DELETE_ALL_VERSIONS);
    }

    @Test
    public void updateEventObjectDeleted() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        StatusEvent updatedEvent = weh.updateEvent(event);

        // since deleted object check fails (objid 1L is not present), we expect to get back an updated event
        Assert.assertNotSame("expected different event object", event, updatedEvent);
        Assert.assertEquals(updatedEvent.getEventType(), StatusEventType.DELETE_ALL_VERSIONS);
    }

    @Test
    public void updateEventObjectDeletedExceptionCase1() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenThrow(new IOException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (RetriableIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectDeletedExceptionCase2() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenThrow(new JsonClientException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (UnprocessableEventIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectDeletedExceptionCase3() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenThrow(new IOException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (RetriableIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }


    @Test
    public void updateEventObjectDeletedExceptionCase4() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newName")
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "r", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenThrow(new JsonClientException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (UnprocessableEventIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectRenamedAndIsPublic() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList)))
                // for isPublic check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        StatusEvent updatedEvent = weh.updateEvent(event);

        // since name and isPublic flag are different in the mocked workspace and object, we expect to get back an updated event
        Assert.assertNotSame("expected different event object", event, updatedEvent);
        Assert.assertEquals("newNameInWorkSpace", updatedEvent.getNewName().get());
        Assert.assertEquals(Boolean.FALSE, updatedEvent.isPublic().get());
    }

    @Test
    public void updateEventObjectRenamedAndIsPublicWithStorageCode() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                "WS",
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList)))
                // for isPublic check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        StatusEvent updatedEvent = weh.updateEvent(event);

        // since name and isPublic flag are different in the mocked workspace and object, we expect to get back an updated event
        Assert.assertNotSame("expected different event object", event, updatedEvent);
        Assert.assertEquals("newNameInWorkSpace", updatedEvent.getNewName().get());
        Assert.assertEquals(Boolean.FALSE, updatedEvent.isPublic().get());
    }

    @Test
    public void updateEventObjectRenamedAndIsPublicExceptionCase1() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenThrow(new IOException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (RetriableIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectRenamedAndIsPublicExceptionCase2() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenThrow(new JsonClientException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (UnprocessableEventIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectRenamedAndIsPublicExceptionCase3() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList)))
                // for isPublic check
                .thenThrow(new IOException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (RetriableIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }

    @Test
    public void updateEventObjectRenamedAndIsPublicExceptionCase4() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient cloned = mock(WorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClientClone()).thenReturn(cloned);
        when(clonecli.getClient()).thenReturn(wscli);

        StatusEvent event = StatusEvent.getBuilder(
                StorageObjectType.fromNullableVersion("WS", "foo", 1),
                Instant.ofEpochMilli(20000),
                StatusEventType.RENAME_ALL_VERSIONS)
                .withNullableNewName("newNameInEvent")
                .withNullableisPublic(Boolean.TRUE)
                .withNullableAccessGroupID(1)
                .withNullableObjectID("1")
                .withNullableVersion(1)
                .build();

        List<Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> objList = new ArrayList<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String,String>>>();
        objList.add(objTuple(1L, "newNameInWorkSpace", "sometype", "date", 1L, "copier",
                1L, "wsname", "checksum", 44, Collections.emptyMap()));

        when(wscli.administer(any()))
                // for deleted workspace check
                .thenReturn(new UObject(wsTuple(1L, "wsname", "username", "date", 7, "n", "n",
                        "unlocked", Collections.emptyMap())))
                // for deleted objects check
                .thenReturn(new UObject(objList))
                // for name check
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList)))
                // for isPublic check
                .thenThrow(new JsonClientException("Test exception"));

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);

        // update event
        boolean exceptionCaught = false;
        try {
            weh.updateEvent(event);
        } catch (UnprocessableEventIndexingException ex) {
            exceptionCaught = true;
        } finally {
            Assert.assertTrue(exceptionCaught);
        }
    }
}
