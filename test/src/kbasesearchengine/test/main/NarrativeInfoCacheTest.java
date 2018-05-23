package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.io.IOException;
import us.kbase.common.service.JsonClientException;
import kbasesearchengine.main.NarrativeInfo;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.main.WorkspaceNarrativeInfoProvider;
import kbasesearchengine.main.NarrativeInfoCache;
import kbasesearchengine.main.NarrativeInfoProvider;
import kbasesearchengine.test.common.TestCommon;

public class NarrativeInfoCacheTest {

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructor() throws Exception {
        // test the non-test constructor
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                1,
                10000);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                new NarrativeInfo(null, null, 1518126945000L, "owner1"),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        Thread.sleep(1001);

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                new NarrativeInfo(null, null, 1518126945000L, "owner1"),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheLookupException() throws Exception {
        // test the non-test constructor

        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        try {
            cache.findNarrativeInfo(65L);
            fail("CacheLoader DID NOT throw an exception for a lookup of non-existing key.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("CacheLoader returned null for key 65."));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                new NarrativeInfo(null, null, 1518126945000L, "owner1"),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));

        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(wrapped, 10000, 15);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                new NarrativeInfo(null, null, 1518126945000L, "owner1"),
                new NarrativeInfo(null, null, 1518126957000L, "owner11"),
                new NarrativeInfo(null, null, 1518126959000L, "owner111"),
                null);
        when(wrapped.findNarrativeInfo(2L)).thenReturn(
                new NarrativeInfo("narrname2", 2L, 20000L, "owner2"),
                new NarrativeInfo("narrname22", 22L, 22000L, "owner22"),
                new NarrativeInfo("narrname222", 222L, 222000L, "owner222"),
                null);
        when(wrapped.findNarrativeInfo(3L)).thenReturn(
                new NarrativeInfo("narrname3", 3L, 30000L, "owner3"),
                new NarrativeInfo("narrname33", 33L, 33000L, "owner33"),
                new NarrativeInfo("narrname333", 333L, 333000L, "owner333"),
                null);
        when(wrapped.findNarrativeInfo(42L)).thenReturn(
                new NarrativeInfo(null, null, 1518126945678L, "user1"),
                new NarrativeInfo("mylovelynarrative", 4L, 1518126950678L, "user2"),
                new NarrativeInfo("mytestnarrative", 44L, 1518126950678L, "user3"),
                null);

        // load 12 narrative infos into a max 15 cache
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(2L),
                new NarrativeInfo("narrname2", 2L, 20000L, "owner2"));
        compare(cache.findNarrativeInfo(3L),
                new NarrativeInfo("narrname3", 3L, 30000L, "owner3"));

        // check cache access
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(2L),
                new NarrativeInfo("narrname2", 2L, 20000L, "owner2"));
        compare(cache.findNarrativeInfo(3L),
                new NarrativeInfo("narrname3", 3L, 30000L, "owner3"));

        // force an expiration based on cache size
        compare(cache.findNarrativeInfo(42L),
                new NarrativeInfo(null, null, 1518126945678L, "user1"));

        // check that with every access, the oldest value had expired
        // 65 had expired. When this is loaded, 2 would be removed from cache
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126957000L, "owner11"));
        // 2 is not in cache. Its next value is cached. So 3 would be removed from cache
        compare(cache.findNarrativeInfo(2L),
                new NarrativeInfo("narrname22", 22L, 22000L, "owner22"));
        // 3 is not in cache. Its next value is cached. So 42 would be removed from cache
        compare(cache.findNarrativeInfo(3L),
                new NarrativeInfo("narrname33", 33L, 33000L, "owner33"));
        // 42 is not in cache. Its next value is cached. So now 65 would be removed from cache
        compare(cache.findNarrativeInfo(42L),
                new NarrativeInfo("mylovelynarrative", 4L, 1518126950678L, "user2"));

        // 65 not in cache. Its next value is cached. But this removes 2 from cache
        compare(cache.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126959000L, "owner111"));
        // 3 is still in cache with the same value
        compare(cache.findNarrativeInfo(3L),
                new NarrativeInfo("narrname33", 33L, 33000L, "owner33"));
        // 42 is still in cache with the same value
        compare(cache.findNarrativeInfo(42L),
                new NarrativeInfo("mylovelynarrative", 4L, 1518126950678L, "user2"));
        // 2 not in cache. The next value of 2 is cached now. 65 is removed from cache
        compare(cache.findNarrativeInfo(2L),
                new NarrativeInfo("narrname222", 222L, 222000L, "owner222"));
        // 3 is in cache
        compare(cache.findNarrativeInfo(3L),
                new NarrativeInfo("narrname33", 33L, 33000L, "owner33"));
        //42 is in cache
        compare(cache.findNarrativeInfo(42L),
                new NarrativeInfo("mylovelynarrative", 4L, 1518126950678L, "user2"));
    }

    @Test
    public void constructFail() throws Exception {
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final NarrativeInfoProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new NarrativeInfoCache(provider, lifetimeSec, size);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, exception);
        }
    }

    private static void compare(
            final NarrativeInfo got,
            final NarrativeInfo expected) {
        assertThat("incorrect narrative name", got.getNarrativeName(), is(expected.getNarrativeName()));
        assertThat("incorrect narrative id", got.getNarrativeId(), is(expected.getNarrativeId()));
        assertThat("incorrect epoch", got.getTimeLastSaved(), is(expected.getTimeLastSaved()));
        assertThat("incorrect owner", got.getWsOwnerUsername(), is(expected.getWsOwnerUsername()));
    }

    @Test
    public void testWorkspaceNarrativeInfoProvider() throws Exception {

        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);

        final NarrativeInfoProvider wsnip = new WorkspaceNarrativeInfoProvider(weh);

        // no narrative info at all
        when(weh.getWorkspaceInfo(65)).thenReturn(wsTuple(
                65, "name1", "owner1", "2018-02-08T21:55:45Z", 0, "r", "n", "unlocked",
                Collections.emptyMap()));

        // only narrative id
        when(weh.getWorkspaceInfo(1)).thenReturn(wsTuple(
                1, "name2", "owner2", "2018-02-08T21:55:57Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "2")));

        // only narrative name
        when(weh.getWorkspaceInfo(2)).thenReturn(wsTuple(
                2, "name3", "owner3", "2018-02-08T21:55:45.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative_nice_name", "myhorridnarrative")));

        // full narrative info
        when(weh.getWorkspaceInfo(42)).thenReturn(wsTuple(
                42, "name4", "owner4", "2018-02-08T21:55:50.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "3", "narrative_nice_name", "mylovelynarrative")));

        compare(wsnip.findNarrativeInfo(65L),
                new NarrativeInfo(null, null, 1518126945000L, "owner1"));
        compare(wsnip.findNarrativeInfo(1L),
                new NarrativeInfo(null, null, 1518126957000L, "owner2"));
        compare(wsnip.findNarrativeInfo(2L),
                new NarrativeInfo(null, null, 1518126945678L, "owner3"));
        compare(wsnip.findNarrativeInfo(42L),
                new NarrativeInfo("mylovelynarrative", 3L, 1518126950678L, "owner4"));
    }

    @Test
    public void wsNarrativeInfoProviderFail() throws Exception {
        failFindNarrativeInfo(new IOException("Test IO Exception"),
                new IOException("Failed retrieving workspace info: Test IO Exception"));
        failFindNarrativeInfo(new JsonClientException("workspace is turned off"),
                new JsonClientException(
                        "Failed retrieving workspace info: workspace is turned off"));
    }

    private void failFindNarrativeInfo(
            final Exception toThrow,
            final Exception expected)
            throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final WorkspaceNarrativeInfoProvider wsnip = new WorkspaceNarrativeInfoProvider(weh);

        when(weh.getWorkspaceInfo(65L)).thenThrow(toThrow);

        try {
            wsnip.findNarrativeInfo(65L);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
