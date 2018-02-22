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

import kbasesearchengine.authorization.AuthCache;
import kbasesearchengine.authorization.AuthInfoProvider;
import kbasesearchengine.test.common.TestCommon;

public class AuthCacheTest {

    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructor() throws Exception {
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

//        when(wrapped.findUserDisplayName("user1")).thenReturn(
//                "display1", "display11", null);
//        when(wrapped.findUserDisplayName("user2")).thenReturn(
//                "display2", "display22", null);

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
    public void standardConstructor2() throws Exception {
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
}
