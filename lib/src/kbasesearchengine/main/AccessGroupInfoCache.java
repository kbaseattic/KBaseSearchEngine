package kbasesearchengine.main;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import kbasesearchengine.tools.Utils;

import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import us.kbase.common.service.Tuple9;

/** A caching layer for workspace info. Caches the results in memory for quick access.
 * @author gaprice@lbl.gov
 * @author ganapathy@bnl.gov
 */

public class AccessGroupInfoCache implements AccessGroupInfoProvider {

    private final LoadingCache<Long, Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>>> cache;

    /** Create a cache.
     * @param provider the {@link AccessGroupInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of workspace info for a user should
     * remain in the cache.
     * @param cacheSizeInWorkspaceInfo the maximum number of workspace info, across all users, to
     * store in the cache.
     */
    public AccessGroupInfoCache(
            final AccessGroupInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInWorkspaceInfo) {
        this(provider, cacheLifeTimeInSec, cacheSizeInWorkspaceInfo, Ticker.systemTicker());
    }

    /** Create a cache for testing purposes.
     * @param cacheLifeTimeInSec the number of seconds a set of workspace info, across all users should
     * remain in the cache.
     * @param cacheSizeInAccessGroupInfo the maximum number of workspace info, across all users, to
     * store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public AccessGroupInfoCache(
            final AccessGroupInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInAccessGroupInfo,
            final Ticker ticker) {

        Utils.nonNull(provider, "provider");

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInAccessGroupInfo < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInAccessGroupInfo)
                .weigher(new Weigher<Long, Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>>>() {

                    @Override
                    public int weigh(Long wsId, Tuple9<Long, String, String, String, Long, String,
                            String, String, Map<String, String>> accessGroupInfo)
                    {
                        return 8 + accessGroupInfo.getE9().size();
                    }
                })
                .build(new CacheLoader<Long, Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>>>() {

                    @Override
                    public Tuple9<Long, String, String, String, Long, String,
                            String, String, Map<String, String>> load(Long accessGroupId) {
                        return provider.getAccessGroupInfo(accessGroupId);
                    }
                });
    }

    @Override
    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> getAccessGroupInfo(final Long accessGroupId) {
        try {
            return cache.get(accessGroupId);
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass()).error("ERROR: Failed retrieving access group info: Returning null:  {}",
                    e.getMessage());
            // unchecked exceptions are wrapped in UncheckedExcecutionException
            return null;
        }
    }
}
