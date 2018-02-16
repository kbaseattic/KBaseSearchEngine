package kbasesearchengine.test.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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

    public static void compare(
            final Map<Long, Tuple5<String, Long, Long, String, String>> got,
            final Map<Long, Tuple5<String, Long, Long, String, String>> expected) {
        assertThat("incorrect map keys", got.keySet(), is(expected.keySet()));
        for (final Long key: got.keySet()) {
            compare(got.get(key), expected.get(key));
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
    
    // TODO  modifying the rest of the unit tests. Will add them.
}