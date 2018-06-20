package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.io.IOException;
import java.util.Map;

import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple9;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;

import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.compareWsInfo;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.main.AccessGroupInfoProvider;
import kbasesearchengine.main.AccessGroupNarrativeInfoProvider;
import kbasesearchengine.main.AccessGroupInfoCache;
import kbasesearchengine.test.common.TestCommon;

public class AccessGroupInfoCacheTest {

    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_v1 = wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                    "r", "unlocked",Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_v2 = wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                    "r", "unlocked", Collections.emptyMap());

    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var1 = wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                    "r", "unlocked", Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var11 = wsTuple(65, "myws11", "owner11", "date11", 32, "a",
                    "r", "unlocked", Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var111 = wsTuple(65, "myws111", "owner111", "date111", 32, "a",
                    "r", "unlocked", Collections.emptyMap());

    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var2 = wsTuple(2, "myws2", "owner2", "date2", 32, "a",
                    "r", "unlocked", ImmutableMap.of("narrative", "3", "narrative_nice_name", "mynarrative"));
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var22 = wsTuple(2, "myws22", "owner22", "date22", 32, "a",
                    "r", "unlocked", Collections.emptyMap());
    private final Tuple9<Long, String, String, String, Long, String, String, String,
            Map<String, String>> wsTuple_65_var222 = wsTuple(2, "myws222", "owner222", "date222", 32, "a",
                    "r", "unlocked", Collections.emptyMap());

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructor() throws Exception {
        // test the non-test constructor
        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(
                wrapped,
                1,
                10000);

        when(wrapped.getAccessGroupInfo(65L)).thenReturn(
                wsTuple_65_v1, wsTuple_65_v2, null);

        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v1);
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v1);
        // sleep little more than cacheLifeTimeInSec
        Thread.sleep(1001);

        // check if the cache data had expired
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v2);
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getAccessGroupInfo(65L)).thenReturn(
                wsTuple_65_v1, wsTuple_65_v2, null);
        // ticker returns little more than mutiples of cacheLifeTimeInSec
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        // because of ticker values, the data in cache should expire on every get
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v1);
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheLookupException() throws Exception {
        // test the non-test constructor

        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(
                wrapped,
                10,
                10000,
                ticker);
        assertNull(cache.getAccessGroupInfo(65L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getAccessGroupInfo(65L)).thenReturn(wsTuple_65_v1, wsTuple_65_v2, null);
        // ticker values are multiples of half of cacheLifeTimeInSec
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        // check to see if cache expires after 2 gets
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v1);
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v1);

        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v2);
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_v2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(wrapped, 10000, 16);

        when(wrapped.getAccessGroupInfo(65L)).thenReturn(
                wsTuple_65_var1, wsTuple_65_var11, wsTuple_65_var111, null);
        when(wrapped.getAccessGroupInfo(2L)).thenReturn(
                wsTuple_65_var2, wsTuple_65_var22, wsTuple_65_var222, null);

        // load 18 workspace infos into a max 16 cache
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_var1);
        // check cache access
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_var1);
        // force an expiration based on cache size
        compareWsInfo(cache.getAccessGroupInfo(2L), wsTuple_65_var2);
        // check that with every access, the oldest value had expired
        compareWsInfo(cache.getAccessGroupInfo(65L), wsTuple_65_var11);
        compareWsInfo(cache.getAccessGroupInfo(2L), wsTuple_65_var22);
    }

    @Test
    public void constructFail() throws Exception {
        final AccessGroupInfoProvider wrapped = mock(AccessGroupInfoProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final AccessGroupInfoProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new AccessGroupInfoCache(provider, lifetimeSec, size);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, exception);
        }
    }

    @Test
    public void wsInfoProviderFail() throws Exception {
        failGetWorkspaceInfo(new IOException("Test IO Exception"),
                new IOException("Test IO Exception"));
        failGetWorkspaceInfo(new JsonClientException("workspace is turned off"),
                new JsonClientException(
                        "workspace is turned off"));
    }

    private void failGetWorkspaceInfo(
            final Exception toThrow,
            final Exception expected)
            throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);

        when(weh.getWorkspaceInfo(65L)).thenThrow(toThrow);

        try {
            weh.getWorkspaceInfo(65L);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    @Test
    public void failAccessGroupInfoProvider() throws Exception {
        failGetAccessGroupsInfo(new IOException("Test IO Exception"));
        failGetAccessGroupsInfo(new JsonClientException("workspace is turned off"));
    }

    private void failGetAccessGroupsInfo(
            final Exception toThrow)
            throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final AccessGroupInfoProvider agip = new AccessGroupNarrativeInfoProvider(weh);
        final AccessGroupInfoCache cache = new AccessGroupInfoCache(agip, 10000, 15);

        when(weh.getWorkspaceInfo(65L)).thenThrow(toThrow);

        assertNull(agip.getAccessGroupInfo(65L));

        assertNull(cache.getAccessGroupInfo(65L));
    }
}
