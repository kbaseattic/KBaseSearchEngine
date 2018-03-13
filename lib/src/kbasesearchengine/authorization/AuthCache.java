package kbasesearchengine.authorization;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

/** A caching layer for auth info. Caches the results from the wrapped
 * {@link AuthInfoProvider} in memory for quick access.
 * @author gaprice@lbl.gov
 * @author ganapathy@bnl.gov
 *
 */
public class AuthCache implements AuthInfoProvider {

    private final LoadingCache<String, String> cache;

    /** Create a cache.
     * @param provider the {@link AuthInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of auth info for a user should
     * remain in the cache.
     * @param cacheSizeInAuthInfo the maximum number of auth info (display name), across all users,
     * to store in the cache.
     */
    public AuthCache(
            final AuthInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInAuthInfo) {
        this(provider, cacheLifeTimeInSec, cacheSizeInAuthInfo, Ticker.systemTicker());
    }

    /** Create a cache for testing purposes.
     * @param provider the {@link AccessGroupProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds an set of access groups for a user should
     * remain in the cache.
     * @param cacheSizeInAuthInfo the maximum number of auth info (display name), across all users,
     * to store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public AuthCache(
            final AuthInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInAuthInfo,
            final Ticker ticker) {
        if (provider == null) {
            throw new NullPointerException("provider");
        }

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInAuthInfo < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInAuthInfo)
                .weigher(new Weigher<String, String>() {

                    @Override
                    public int weigh(String userName, String displayName) {
                        // TODO check what needs to be returned
                        return 1;
                    }
                })
                .build(new CacheLoader<String, String> () {

                    @Override
                    public String load(String userName) throws Exception {
                        return provider.findUserDisplayName(userName);
                    }

                    @Override
                    public Map<String, String> loadAll(Iterable<? extends String> userNames) throws Exception {
                        return provider.findUserDisplayNames(userNames);
                    }
                 });
    }

    @Override
    public String findUserDisplayName(final String userName)
            throws IOException, Auth2Exception {
        try {
            return cache.get(userName);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }

    @Override
    public Map<String, String> findUserDisplayNames(final Iterable <? extends String> userNames)
            throws IOException, Auth2Exception {
        try {
            return cache.getAll(userNames);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }
}
