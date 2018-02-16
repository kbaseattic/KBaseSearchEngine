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

    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final NarrativeInfoProvider wrapped = mock(NarrativeInfoProvider.class);
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                10000,
                10);

        when(wrapped.findNarrativeInfo("foo")).thenReturn(
                Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(2, 3), null);
        when(wrapped.findNarrativeInfo("bar")).thenReturn(
                Arrays.asList(6, 7, 8, 9), Arrays.asList(11, 12), null);
        when(wrapped.findNarrativeInfo("baz")).thenReturn(
                Arrays.asList(20, 21), Arrays.asList(22, 23), null);

        // load 9 access group IDs into a max 10 cache
        assertThat("incorrect narrative info", cache.findNarrativeInfo("foo"),
                is(Arrays.asList(1, 2, 3, 4, 5)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo("bar"),
                is(Arrays.asList(6, 7, 8, 9)));

        // check cache access
        assertThat("incorrect narrative info", cache.findNarrativeInfo("foo"),
                is(Arrays.asList(1, 2, 3, 4, 5)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo("bar"),
                is(Arrays.asList(6, 7, 8, 9)));

        // force an expiration based on cache size
        assertThat("incorrect narrative info", cache.findNarrativeInfo("baz"),
                is(Arrays.asList(20, 21)));

        // check that the largest value was expired
        assertThat("incorrect narrative info", cache.findNarrativeInfo("foo"),
                is(Arrays.asList(2, 3)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo("bar"),
                is(Arrays.asList(6, 7, 8, 9)));
        assertThat("incorrect narrative info", cache.findNarrativeInfo("baz"),
                is(Arrays.asList(20, 21)));
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
        final NarrativeInfoCache cache = new NarrativeInfoCache(
                wrapped,
                10000,
                10);

        when(wrapped.findNarrativeInfo(65L)).thenThrow(new IOException("well poop"));

        failFindNarrativeInfo(cache, 65L, new IOException("well poop"));
    }

    private void failFindNarrativeInfo(
            final NarrativeInfoCache cache,
            final Long wsId,
            final Exception expected) {
        try {
            cache.findNarrativeInfo(user);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }


    @Test
    public void searchObjectsDecorateWithNullInfo() throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final SearchInterface search = mock(SearchInterface.class);

        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, weh, authURLString, token2);

        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */

        final SearchObjectsInput dummyInput = new SearchObjectsInput();

        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(null));

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

        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");

        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = ImmutableMap.of(
                65L, narrInfo(null, null, 1518126945000L, "owner1", null),
                1L, narrInfo(null, null, 1518126957000L, "owner2", null),
                2L, narrInfo(null, null, 1518126945678L, "owner3", null),
                42L, narrInfo("mylovelynarrative", 3L, 1518126950678L, "owner4", null));

        assertThat("incorrect object data", res.getObjects(), is(objectdata));

        compare(res.getAccessGroupNarrativeInfo(), expected);
    }

    // TODO  modifying the rest of the unit tests. Will add them.
}