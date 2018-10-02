package kbasesearchengine.main;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kbasesearchengine.tools.Utils;

import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import us.kbase.common.service.JsonClientException;

/** A caching layer for workspace and narrative info. Caches the results in memory for quick access.
 * @author gaprice@lbl.gov
 * @author ganapathy@bnl.gov
 */

public class NarrativeInfoCache implements NarrativeInfoProvider {

    private final LoadingCache<Long, NarrativeInfo> cache;

    /** Create a cache.
     * @param provider the {@link NarrativeInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of narrative info for a user should
     * remain in the cache.
     * @param cacheSizeInNarrativeInfo the maximum number of narrative info, across all users, to
     * store in the cache.
     */
    public NarrativeInfoCache(
            final NarrativeInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInNarrativeInfo) {
        this(provider, cacheLifeTimeInSec, cacheSizeInNarrativeInfo, Ticker.systemTicker());
    }

    /** Create a cache for testing purposes.
     * @param cacheLifeTimeInSec the number of seconds an set of narrative info for a user should
     * remain in the cache.
     * @param cacheSizeInNarrativeInfo the maximum number of narrative info, across all users, to
     * store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public NarrativeInfoCache(
            final NarrativeInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInNarrativeInfo,
            final Ticker ticker) {

        Utils.nonNull(provider, "provider");

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInNarrativeInfo < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInNarrativeInfo)
                .weigher(new Weigher<Long, NarrativeInfo>() {

                    @Override
                    public int weigh(Long wsId, NarrativeInfo narrativeInfo)
                    {
                        return narrativeInfo.getNumElements();
                    }
                })
                .build(new CacheLoader<Long, NarrativeInfo>() {

                    @Override
                    public NarrativeInfo load(Long wsId) throws Exception {
                        return provider.findNarrativeInfo(wsId);
                    }
                });
    }

    @Override
    public NarrativeInfo findNarrativeInfo(final Long wsId) throws IOException, JsonClientException {
        try {
            return cache.get(wsId);
        } catch (ExecutionException e) {
            final Throwable ex = e.getCause();
            if (ex instanceof IOException ) {
                throw (IOException) ex;
            } else if (ex instanceof JsonClientException) {
                throw (JsonClientException) ex;
            } else {
                LoggerFactory.getLogger(getClass()).error("This exception should not happen: {}",
                        ex.getMessage());
                return null;
            }
        } catch (Exception ex) {
            LoggerFactory.getLogger(getClass()).error("ERROR: Failed retrieving narrative info: Returning null:  {}",
                    ex.getMessage());
            return null;
        }
    }
}
