package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.main.NarrativeInfoCache;
import kbasesearchengine.main.NarrativeInfoProvider;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.Tuple5;

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
                narrInfo(null, null, 1518126945000L, "owner1", null),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null),
                null);

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));
        Thread.sleep(1001);

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null)));
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
                narrInfo(null, null, 1518126945000L, "owner1", null),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null),
                null);
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null)));
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
                narrInfo(null, null, 1518126945000L, "owner1", null),
                narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null),
                null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));

        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo("MyNarrativeName", 3L, 1518126957000L, "owner2", null)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(wrapped, 10000, 10);

        when(wrapped.findNarrativeInfo(65L)).thenReturn(
                narrInfo(null, null, 1518126945000L, "owner1", null),
                narrInfo(null, null, 1518126957000L, "owner2", null),
                null);
        when(wrapped.findNarrativeInfo(2L)).thenReturn(
                narrInfo("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld"),
                narrInfo("narrname6", 2L, 20000L, "owner6", "Herbert K. Kornfeld"),
                null);
        when(wrapped.findNarrativeInfo(42L)).thenReturn(
                narrInfo(null, null, 1518126945678L, "user1", "display1"),
                narrInfo("mylovelynarrative", 3L, 1518126950678L, "user2", "display2"),
                null);

        // load 9 narrative infos into a max 10 cache
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(2L),
                is(narrInfo("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld")));

        // check cache access
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126945000L, "owner1", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(2L),
                is(narrInfo("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld")));

        // force an expiration based on cache size
        assertThat("incorrect narrative info", cache.findNarrativeInfo(42L),
                is(narrInfo(null, null, 1518126945678L, "user1", "display1")));

        // check that the largest value was expired
        assertThat("incorrect narrative info", cache.findNarrativeInfo(65L),
                is(narrInfo(null, null, 1518126957000L, "owner2", null)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(2L),
                is(narrInfo("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld")));
        assertThat("incorrect narrative info", cache.findNarrativeInfo(42L),
                is(narrInfo(null, null, 1518126945678L, "user1", "display1")));
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

    public static void compare(
            final Tuple5<String, Long, Long, String, String> got,
            final Tuple5<String, Long, Long, String, String> expected) {
        assertThat("incorrect narrative name", got.getE1(), is(expected.getE1()));
        assertThat("incorrect narrative id", got.getE2(), is(expected.getE2()));
        assertThat("incorrect epoch", got.getE3(), is(expected.getE3()));
        assertThat("incorrect owner", got.getE4(), is(expected.getE4()));
        assertThat("incorrect display name", got.getE5(), is(expected.getE5()));
    }

    public static Tuple5<String, Long, Long, String, String> narrInfo(
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
