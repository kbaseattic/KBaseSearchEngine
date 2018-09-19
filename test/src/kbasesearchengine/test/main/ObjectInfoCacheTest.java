package kbasesearchengine.test.main;

import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.util.Collections;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import us.kbase.common.service.JsonClientException;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.objTuple;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfo;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareObjInfoMap;
import static kbasesearchengine.test.common.TestCommon.set;
import kbasesearchengine.main.ObjectInfoProvider;
import kbasesearchengine.main.ObjectInfoCache;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.Tuple11;

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

        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final ObjectInfoCache cache = new ObjectInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        try {
            cache.getObjectInfo("65/1/1");
            fail("CacheLoader DID NOT throw an exception for a lookup of non-existing key.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("CacheLoader returned null for key 65/1/1."));
        }
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
    public void getFailIOE() throws Exception {
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(wrapped, 10000, 10);

        when(wrapped.getObjectsInfo(set("65/1/1"))).thenThrow(new IOException("Test Exception Message"));
        failFindObjectInfo(cache, ImmutableList.of("65/1/1"), new IOException("Test Exception Message"));
    }


    @Test
    public void getFailIODeleted() throws Exception {
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(wrapped, 10000, 10);

        when(wrapped.getObjectsInfo(set("65/1/1"))).thenThrow(new IOException("is deleted"));
        assertThat("should be null", cache.getObjectsInfo(ImmutableList.of("65/1/1")) == null, is(true));
    }

    @Test
    public void getFailJSONE() throws Exception {
        final ObjectInfoProvider wrapped = mock(ObjectInfoProvider.class);
        final ObjectInfoCache cache = new ObjectInfoCache(wrapped, 10000, 10);

        when(wrapped.getObjectsInfo(set("65/1/1"))).thenThrow(new JsonClientException("Test Exception Message"));
        failFindObjectInfo(cache, ImmutableList.of("65/1/1"), new JsonClientException("Test Exception Message"));
    }

    private void failFindObjectInfo(
            final ObjectInfoCache cache,
            final ImmutableList<String> objRefs,
            final Exception expected) {
        try {
            cache.getObjectsInfo(objRefs);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
