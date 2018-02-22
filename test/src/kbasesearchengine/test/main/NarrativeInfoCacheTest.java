package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import kbasesearchengine.main.NarrativeInfo;
import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Ticker;

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
                narrInfo(null, null, 1518126945000L, "owner1"),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);

        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));
        Thread.sleep(1001);

        compare(cache.findNarrativeInfo(65L),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
        compare(cache.findNarrativeInfo(65L),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
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
                narrInfo(null, null, 1518126945000L, "owner1"),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));

        compare(cache.findNarrativeInfo(65L),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
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
                narrInfo(null, null, 1518126945000L, "owner1"),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"),
                null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));

        compare(cache.findNarrativeInfo(65L),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
        compare(cache.findNarrativeInfo(65L),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(wrapped, 10000, 10);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                narrInfo(null, null, 1518126945000L, "owner1"),
                narrInfo(null, null, 1518126957000L, "owner2"),
                null);
        when(wrapped.findNarrativeInfo(2L)).thenReturn(
                narrInfo("narrname", 1L, 10000L, "owner"),
                narrInfo("narrname6", 2L, 20000L, "owner6"),
                null);
        when(wrapped.findNarrativeInfo(42L)).thenReturn(
                narrInfo(null, null, 1518126945678L, "user1"),
                narrInfo("mylovelynarrative", 3L, 1518126950678L, "user2"),
                null);

        // load 9 narrative infos into a max 10 cache
        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(2L),
                narrInfo("narrname", 1L, 10000L, "owner"));

        // check cache access
        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126945000L, "owner1"));
        compare(cache.findNarrativeInfo(2L),
                narrInfo("narrname", 1L, 10000L, "owner"));

        // force an expiration based on cache size
        compare(cache.findNarrativeInfo(42L),
                narrInfo(null, null, 1518126945678L, "user1"));

        // check that the largest value was expired
        compare(cache.findNarrativeInfo(65L),
                narrInfo(null, null, 1518126957000L, "owner2"));
        compare(cache.findNarrativeInfo(2L),
                narrInfo("narrname", 1L, 10000L, "owner"));
        compare(cache.findNarrativeInfo(42L),
                narrInfo(null, null, 1518126945678L, "user1"));
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

    @Test
    public void getFailIOE() throws Exception {
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(wrapped, 10000, 10);

        when(wrapped.findNarrativeInfo(23L)).thenThrow(new IOException("well poop"));

        findNarrativeInfo(cache, 23L, new IOException("well poop"));
    }

    private void findNarrativeInfo(
            final NarrativeInfoCache cache,
            final Long wsId,
            final Exception expected) {
        try {
            cache.findNarrativeInfo(wsId);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
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

    private static NarrativeInfo narrInfo(
            final String narrativeName,
            final Long narrativeId,
            final Long epoch,
            final String owner) {
        return new NarrativeInfo()
                .withNarrativeName(narrativeName)
                .withNarrativeId(narrativeId)
                .withTimeLastSaved(epoch)
                .withWsOwnerUsername(owner);
    }
}
