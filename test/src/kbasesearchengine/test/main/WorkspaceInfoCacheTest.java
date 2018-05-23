package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.io.IOException;
import us.kbase.common.service.JsonClientException;
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
import kbasesearchengine.main.WorkspaceInfoProvider;
import kbasesearchengine.main.WorkspaceInfoCache;
import kbasesearchengine.test.common.TestCommon;

public class WorkspaceInfoCacheTest {

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructor() throws Exception {
        // test the non-test constructor
        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        final WorkspaceInfoCache cache = new WorkspaceInfoCache(
                wrapped,
                1,
                10000);

        when(wrapped.getWorkspaceInfo(65L)).thenReturn(
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                "r", "unlocked", Collections.emptyMap()),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                "r", "unlocked", Collections.emptyMap()),
                null);

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        Thread.sleep(1001);

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final WorkspaceInfoCache cache = new WorkspaceInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getWorkspaceInfo(65L)).thenReturn(
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheLookupException() throws Exception {
        // test the non-test constructor

        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final WorkspaceInfoCache cache = new WorkspaceInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        try {
            cache.getWorkspaceInfo(65L);
            fail("CacheLoader DID NOT throw an exception for a lookup of non-existing key.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("CacheLoader returned null for key 65."));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final WorkspaceInfoCache cache = new WorkspaceInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.getWorkspaceInfo(65L)).thenReturn(
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));

        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        final WorkspaceInfoCache cache = new WorkspaceInfoCache(wrapped, 10000, 16);

        when(wrapped.getWorkspaceInfo(65L)).thenReturn(
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(65, "myws11", "owner11", "date11", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(65, "myws111", "owner111", "date111", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);
        when(wrapped.getWorkspaceInfo(2L)).thenReturn(
                wsTuple(2, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(2, "myws22", "owner22", "date22", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(2, "myws222", "owner222", "date222", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);
        when(wrapped.getWorkspaceInfo(3L)).thenReturn(
                wsTuple(3, "myws3", "owner3", "date3", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(3, "myws33", "owner33", "date33", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(3, "myws333", "owner333", "date333", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);
        when(wrapped.getWorkspaceInfo(42L)).thenReturn(
                wsTuple(42, "myws4", "owner4", "date4", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(42, "myws44", "owner44", "date44", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                wsTuple(42, "myws444", "owner444", "date444", 32, "a",
                        "r", "unlocked", Collections.emptyMap()),
                null);

        // load 18 workspace infos into a max 16 cache
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        // check cache access
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws1", "owner1", "date1", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        // force an expiration based on cache size
        compareWsInfo(cache.getWorkspaceInfo(2L),
                wsTuple(2, "myws2", "owner2", "date2", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        // check that with every access, the oldest value had expired
        compareWsInfo(cache.getWorkspaceInfo(65L),
                wsTuple(65, "myws11", "owner11", "date11", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
        compareWsInfo(cache.getWorkspaceInfo(2L),
                wsTuple(2, "myws22", "owner22", "date22", 32, "a",
                        "r", "unlocked", Collections.emptyMap()));
    }

    @Test
    public void constructFail() throws Exception {
        final WorkspaceInfoProvider wrapped = mock(WorkspaceInfoProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final WorkspaceInfoProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new WorkspaceInfoCache(provider, lifetimeSec, size);
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
}
