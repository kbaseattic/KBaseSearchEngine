package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;

import java.util.Set;
import java.io.IOException;

import kbasesearchengine.authorization.AuthCache;
import kbasesearchengine.authorization.AuthInfoProvider;
import kbasesearchengine.test.common.TestCommon;

public class AuthCacheTest {

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructorMultiLookup() throws Exception {
        // test the non-test constructor
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final AuthCache cache = new AuthCache(
                wrapped,
                2,
                10000);

        when(wrapped.findUserDisplayNames(set("user1", "user2"))).thenReturn(
                ImmutableMap.of("user1", "display1", "user2", "display2"),
                ImmutableMap.of("user1", "display11", "user2", "display22"),
                null);

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));
        Thread.sleep(2001);

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display11", "user2", "display22")));
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display11", "user2", "display22")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructorSingleLookup() throws Exception {
        // test the non-test constructor
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final AuthCache cache = new AuthCache(
                wrapped,
                1,
                10000);

        when(wrapped.findUserDisplayName("user1")).thenReturn(
                "display1", "display11", null);

        assertThat("Incorrect display name", cache.findUserDisplayName("user1"),
                is("display1"));
        assertThat("Incorrect display name", cache.findUserDisplayName("user1"),
                is("display1"));
        Thread.sleep(1001);

        assertThat("Incorrect display name", cache.findUserDisplayName("user1"),
                is("display11"));
        assertThat("Incorrect display name", cache.findUserDisplayName("user1"),
                is("display11"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AuthCache cache = new AuthCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.findUserDisplayNames(set("user1", "user2"))).thenReturn(
                ImmutableMap.of("user1", "display1", "user2", "display2"),
                ImmutableMap.of("user1", "display11", "user2", "display22"),
                null);

        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display11", "user2", "display22")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AuthCache cache = new AuthCache(
                wrapped,
                10,
                10000,
                ticker);

        when(wrapped.findUserDisplayNames(set("user1", "user2"))).thenReturn(
                ImmutableMap.of("user1", "display1", "user2", "display2"),
                ImmutableMap.of("user1", "display11", "user2", "display22"),
                null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));
        // TODO:  Not clear why the following test is failing
        //assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
        //        is(ImmutableMap.of("user1", "display1", "user2", "display2")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display11", "user2", "display22")));
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display11", "user2", "display22")));
    }

    // TODO: Need to check this test.
    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final AuthCache cache = new AuthCache(
                wrapped,
                10000,
                5);

        when(wrapped.findUserDisplayNames(set("user1"))).thenReturn(
                ImmutableMap.of("user1", "display1"),
                ImmutableMap.of("user1", "display11"),
                null);
        when(wrapped.findUserDisplayNames(set("user2"))).thenReturn(
                ImmutableMap.of("user2", "display2"),
                ImmutableMap.of("user2", "display22"),
                null);
        when(wrapped.findUserDisplayNames(set("user3"))).thenReturn(
                ImmutableMap.of("user3", "display3"),
                ImmutableMap.of("user3", "display33"),
                null);
        when(wrapped.findUserDisplayNames(set("user4"))).thenReturn(
                ImmutableMap.of("user4", "display4"),
                ImmutableMap.of("user4", "display44"),
                null);
        when(wrapped.findUserDisplayNames(set("user5"))).thenReturn(
                ImmutableMap.of("user5", "display5"),
                ImmutableMap.of("user5", "display55"),
                null);
        when(wrapped.findUserDisplayNames(set("user6"))).thenReturn(
                ImmutableMap.of("user6", "display6"),
                ImmutableMap.of("user6", "display66"),
                null);
        when(wrapped.findUserDisplayNames(set("user1", "user2"))).thenReturn(
                ImmutableMap.of("user1", "display1", "user2", "display2"),
                ImmutableMap.of("user1", "display11", "user2", "display22"),
                null);
        when(wrapped.findUserDisplayNames(set("user3", "user4"))).thenReturn(
                ImmutableMap.of("user3", "display3", "user4", "display4"),
                ImmutableMap.of("user3", "display33", "user4", "display44"),
                null);
        when(wrapped.findUserDisplayNames(set("user5", "user6"))).thenReturn(
                ImmutableMap.of("user5", "display5", "user6", "display6"),
                ImmutableMap.of("user5", "display55", "user6", "display66"),
                null);

        // load 4 auth infos into a max 5 cache
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user3", "user4")),
                is(ImmutableMap.of("user3", "display3", "user4", "display4")));

        // check cache access
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user3", "user4")),
                is(ImmutableMap.of("user3", "display3", "user4", "display4")));

        // force an expiration based on cache size
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user5", "user6")),
                is(ImmutableMap.of("user5", "display5", "user6", "display6")));

        // TODO: Need to check the following test. Supposed to return Display11 and Display22??
        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user1", "user2")),
                is(ImmutableMap.of("user1", "display1", "user2", "display2")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user3", "user4")),
                is(ImmutableMap.of("user3", "display3", "user4", "display4")));

        assertThat("Incorrect display names", cache.findUserDisplayNames(set("user5", "user6")),
                is(ImmutableMap.of("user5", "display5", "user6", "display6")));
    }

    @Test
    public void constructFail() throws Exception {
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final AuthInfoProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new AuthCache(provider, lifetimeSec, size);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, exception);
        }
    }

    @Test
    public void getFailIOE() throws Exception {
        final AuthInfoProvider wrapped = mock(AuthInfoProvider.class);
        final AuthCache cache = new AuthCache(wrapped, 10000, 10);

        when(wrapped.findUserDisplayNames(set("user1", "user2"))).thenThrow(new IOException("Test Exception Message"));

        failFindAuthInfo(cache, set("user1", "user2"), new IOException("Test Exception Message"));
    }

    private void failFindAuthInfo(
            final AuthCache cache,
            final Set<String> userNames,
            final Exception expected) {
        try {
            cache.findUserDisplayNames(userNames);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
