package kbasesearchengine.test.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.util.Collections;
import java.io.IOException;

import kbasesearchengine.events.handler.CloneableWorkspaceClient;
import us.kbase.common.service.JsonClientException;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.objTuple;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfo;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfoMap;
import static kbasesearchengine.test.common.TestCommon.set;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.main.AccessGroupNarrativeInfoProvider;
import kbasesearchengine.main.ObjectInfoProvider;
import kbasesearchengine.main.ObjectInfoCache;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;
import us.kbase.common.service.Tuple11;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.GetObjectInfo3Results;

public class ObjectInfoCacheTest {

    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> obj_65_1_1_var1 =
            objTuple(1L, "objName1", "sometype", "date1", 1L,"copier1",
                    65L, "wsname1", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> obj_65_1_1_var11 =
            objTuple(1L, "objName11", "sometype", "date11", 1L, "copier11",
                    65L, "wsname11", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> obj_2_1_1_var2 =
            objTuple(1L, "objName2", "sometype", "date2", 1L, "copier2",
                    2L, "wsname2", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> obj_2_1_1_var22 =
            objTuple(1L, "objName22", "sometype", "date22", 1L, "copier22",
                    2L, "wsname22", "checksum", 44, Collections.emptyMap());

    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_65_2_7 =
            objTuple(2L, "objName2", "sometype", "date2", 7L,"copier2",
                    65L, "wsname65", "checksum", 44, Collections.emptyMap());
    private final Tuple11<Long, String, String, String, Long, String,
            Long, String, String, Long, Map<String, String>> objTuple_42_7_21 =
            objTuple(7L, "objName7", "sometype", "date7", 21L, "copier7",
                    42L, "wsname42", "checksum", 44, Collections.emptyMap());

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructorMultiLookup() throws Exception {
        // test the non-test constructor
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                2,
                10000);

        when(wrapped.getObjectsInfo(set("65/1/1", "2/1/1"))).thenReturn(
                ImmutableMap.of("65/1/1", obj_65_1_1_var1, "2/1/1", obj_2_1_1_var2),
                ImmutableMap.of("65/1/1", obj_65_1_1_var11, "2/1/1", obj_2_1_1_var22),
                null);

        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "2/1/1", obj_2_1_1_var2,
                        "65/1/1", obj_65_1_1_var1));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2));
        // sleep little more than cacheLifeTimeInSec
        Thread.sleep(2001);
        // check if the cached data had expired
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructorSingleLookup() throws Exception {
        // test the non-test constructor
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                1,
                10000);

        when(wrapped.getObjectsInfo(ImmutableList.of("65/1/1"))).thenReturn(
                ImmutableMap.of("65/1/1", obj_65_1_1_var1),
                ImmutableMap.of("65/1/1", obj_65_1_1_var11),
                null);
        when(wrapped.getObjectInfo("65/1/1")).thenReturn(
                obj_65_1_1_var1,
                obj_65_1_1_var11,
                null);

        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);
        // sleep little more than cacheLifeTimeInSec
        Thread.sleep(1001);
        // check if the cached data had expired
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var11);
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var11);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getObjectsInfo(set("65/1/1", "2/1/1"))).thenReturn(
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22),
                null);
        // ticker returns little more than multiples of cacheLifeTimeInSec
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);
        // because of ticker values, the data in cache should expire on every get
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheLookupException() throws Exception {
        // test the non-test constructor

        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        assertNull(cache.getObjectInfo("65/1/1"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getObjectsInfo(ImmutableList.of("65/1/1"))).thenReturn(
                ImmutableMap.of("65/1/1", obj_65_1_1_var1),
                ImmutableMap.of("65/1/1", obj_65_1_1_var11),
                null);
        when(wrapped.getObjectInfo("65/1/1")).thenReturn(
                obj_65_1_1_var1,
                obj_65_1_1_var11,
                null);
        // ticker values are multiples of half of cacheLifeTimeInSec
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);
        // check to see if cache expires after 2 gets
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);

        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var11);
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var11);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGetMultiple() throws Exception {
        // test that the cache is accessed when available
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getObjectsInfo(set("65/1/1", "2/1/1"))).thenReturn(
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22),
                null);
        // ticker values are multiples of half of cacheLifeTimeInSec, returned twice
        when(ticker.read()).thenReturn(0L, 0L, 5000000001L, 5000000001L, 10000000001L, 10000000001L,
                15000000001L, 15000000001L, 20000000001L, 20000000001L);
        // check to see if the cached data expires after 2 gets
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var1,
                        "2/1/1", obj_2_1_1_var2));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22));
        compareObjInfoMap(cache.getObjectsInfo(set("65/1/1", "2/1/1")),
                ImmutableMap.of(
                        "65/1/1", obj_65_1_1_var11,
                        "2/1/1", obj_2_1_1_var22));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(wrapped, 10000, 16);

        when(wrapped.getObjectInfo("65/1/1")).thenReturn(
                obj_65_1_1_var1,
                obj_65_1_1_var11,
                null);
        when(wrapped.getObjectInfo("2/1/1")).thenReturn(
                obj_2_1_1_var2,
                obj_2_1_1_var22,
                null);

        // load 22 object infos into a max 16 cache
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);
        // check cache access
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var1);
        // force an expiration based on cache size
        compareObjInfo(cache.getObjectInfo("2/1/1"), obj_2_1_1_var2);
        // check that with every access, the oldest value had expired
        compareObjInfo(cache.getObjectInfo("65/1/1"), obj_65_1_1_var11);
        compareObjInfo(cache.getObjectInfo("2/1/1"), obj_2_1_1_var22);
    }

    @Test
    public void constructFail() throws Exception {
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final ObjectInfoProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new ObjectInfoCache(provider, lifetimeSec, size);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, exception);
        }
    }

    @Test
    public void failGetObjectsInfo() throws Exception {
        final CloneableWorkspaceClient clonecli = mock(CloneableWorkspaceClient.class);
        final WorkspaceClient wscli = mock(WorkspaceClient.class);
        when(clonecli.getClient()).thenReturn(wscli);

        final WorkspaceEventHandler weh = new WorkspaceEventHandler(clonecli);
        final ObjectInfoProvider oip = new AccessGroupNarrativeInfoProvider(weh);
        final ObjectInfoCache cache = new ObjectInfoCache(oip, 10000, 15);

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
        pathList.add(null);

        when(wscli.administer(any()))
                .thenReturn(new UObject(new GetObjectInfo3Results().withInfos(objList).withPaths(pathList)));

        Map<String, Tuple11<Long, String, String, String, Long, String,
                Long, String, String, Long, Map<String, String>>> retVal = cache.getObjectsInfo(set("65/2/7", "1/2/1", "42/7/21", "2/2/2"));

        System.out.println("??????????????????????????????   RetVal:  " + retVal);
//        compareObjInfo(retVal.get("42/7/21"), objTuple_42_7_21);
//        compareObjInfo(retVal.get("65/2/7"), objTuple_65_2_7);
//        assertNull("Obj Info not null", retVal.get("1/2/1"));
//        assertNull("Obj Info not null", retVal.get("2/2/2"));
    }
}
