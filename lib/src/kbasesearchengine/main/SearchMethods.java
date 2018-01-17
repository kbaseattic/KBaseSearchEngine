package kbasesearchengine.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import kbasesearchengine.AccessFilter;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.KeyDescription;
import kbasesearchengine.MatchFilter;
import kbasesearchengine.MatchValue;
import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.PostProcessing;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SortingRule;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.authorization.AccessGroupProvider;
import kbasesearchengine.common.GUID;
import kbasesearchengine.search.FoundHits;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.TypeStorage;
import us.kbase.common.service.UObject;

public class SearchMethods {
    
    private final AccessGroupProvider accessGroupProvider;
    private final TypeStorage typeStorage;
    private final IndexingStorage indexingStorage;
    private final Set<String> admins;
    
    public SearchMethods(
            final AccessGroupProvider accessGroupProvider,
            final IndexingStorage indexingStorage,
            final TypeStorage typeStorage,
            final Set<String> admins) {
        this.admins = admins == null ? Collections.emptySet() : admins;
        
        // 50k simultaneous users * 1000 group ids each seems like plenty = 50M ints in memory
        this.accessGroupProvider = accessGroupProvider;
        this.typeStorage = typeStorage;
        this.indexingStorage = indexingStorage;
    }
    
    /**
     * For tests only !!!
     */
    public SearchMethods(
            final IndexingStorage indexingStorage,
            final TypeStorage typeStorage) {
        this.accessGroupProvider = null;
        this.admins = Collections.emptySet();
        this.typeStorage = typeStorage;
        this.indexingStorage = indexingStorage;
    }
    
    public static File getTempSubDir(final File rootTempDir, String subName) {
        File ret = new File(rootTempDir, subName);
        if (!ret.exists()) {
            ret.mkdirs();
        }
        return ret;
    }
    

    public IndexingStorage getIndexingStorage(String objectType) {
        return indexingStorage;
    }
    
    private static boolean toBool(Long value) {
        return value != null && value == 1L;
    }

    private static boolean toBool(Long value, boolean defaultRet) {
        if (value == null) {
            return defaultRet;
        }
        return value == 1L;
    }

    private static Integer toInteger(Long value) {
        return value == null ? null : (int)(long)value;
    }

    private static GUID toGUID(String value) {
        return value == null ? null : new GUID(value);
    }

    private kbasesearchengine.search.MatchValue toSearch(MatchValue mv, String source) {
        if (mv == null) {
            return null;
        }
        if (mv.getValue() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getValue());
        }
        if (mv.getIntValue() != null) {
            return new kbasesearchengine.search.MatchValue(toInteger(mv.getIntValue()));
        }
        if (mv.getDoubleValue() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getDoubleValue());
        }
        if (mv.getBoolValue() != null) {
            return new kbasesearchengine.search.MatchValue(toBool(mv.getBoolValue()));
        }
        if (mv.getMinInt() != null || mv.getMaxInt() != null) {
            return new kbasesearchengine.search.MatchValue(toInteger(mv.getMinInt()),
                    toInteger(mv.getMaxInt()));
        }
        if (mv.getMinDate() != null || mv.getMaxDate() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getMinDate(), mv.getMaxDate());
        }
        if (mv.getMinDouble() != null || mv.getMaxDouble() != null) {
            return new kbasesearchengine.search.MatchValue(mv.getMinDouble(), mv.getMaxDouble());
        }
        throw new IllegalStateException("Unsupported " + source + " filter: " + mv);
    }
    
    private kbasesearchengine.search.MatchFilter toSearch(MatchFilter mf) {
        kbasesearchengine.search.MatchFilter ret = 
                new kbasesearchengine.search.MatchFilter()
                .withFullTextInAll(mf.getFullTextInAll())
                .withAccessGroupId(toInteger(mf.getAccessGroupId()))
                .withObjectName(mf.getObjectName())
                .withParentGuid(toGUID(mf.getParentGuid()))
                .withTimestamp(toSearch(mf.getTimestamp(), "timestamp"));
        if (mf.getLookupInKeys() != null) {
            Map<String, kbasesearchengine.search.MatchValue> keys =
                    new LinkedHashMap<String, kbasesearchengine.search.MatchValue>();
            for (String key : mf.getLookupInKeys().keySet()) {
                keys.put(key, toSearch(mf.getLookupInKeys().get(key), key));
            }
            ret.withLookupInKeys(keys);
        }
        return ret;
    }

    private kbasesearchengine.search.AccessFilter toSearch(AccessFilter af, String user)
            throws IOException {
        List<Integer> accessGroupIds;
        if (toBool(af.getWithPrivate(), true)) {
            accessGroupIds = accessGroupProvider.findAccessGroupIds(user);
        } else {
            accessGroupIds = Collections.emptyList();
        }
        return new kbasesearchengine.search.AccessFilter()
                .withPublic(toBool(af.getWithPublic()))
                .withAllHistory(toBool(af.getWithAllHistory()))
                .withAccessGroups(new LinkedHashSet<>(accessGroupIds))
                .withAdmin(admins.contains(user));
    }
    
    private kbasesearchengine.search.SortingRule toSearch(SortingRule sr) {
        if (sr == null) {
            return null;
        }
        kbasesearchengine.search.SortingRule ret = new kbasesearchengine.search.SortingRule();
        ret.isTimestamp = toBool(sr.getIsTimestamp());
        ret.isObjectName = toBool(sr.getIsObjectName());
        ret.keyName = sr.getKeyName();
        ret.ascending = !toBool(sr.getDescending());
        return ret;
    }

    private SortingRule fromSearch(kbasesearchengine.search.SortingRule sr) {
        if (sr == null) {
            return null;
        }
        SortingRule ret = new SortingRule();
        if (sr.isTimestamp) {
            ret.withIsTimestamp(1L);
        } else if (sr.isObjectName) {
            ret.withIsObjectName(1L);
        } else {
            ret.withKeyName(sr.keyName);
        }
        ret.withDescending(sr.ascending ? 0L : 1L);
        return ret;
    }

    private kbasesearchengine.search.Pagination toSearch(Pagination pg) {
        return pg == null ? null : new kbasesearchengine.search.Pagination(
                toInteger(pg.getStart()), toInteger(pg.getCount()));
    }

    private Pagination fromSearch(kbasesearchengine.search.Pagination pg) {
        return pg == null ? null : new Pagination().withStart((long)pg.start)
                .withCount((long)pg.count);
    }

    private kbasesearchengine.search.PostProcessing toSearch(PostProcessing pp) {
        kbasesearchengine.search.PostProcessing ret = 
                new kbasesearchengine.search.PostProcessing();
        if (pp == null) {
            ret.objectInfo = true;
            ret.objectData = true;
            ret.objectKeys = true;
        } else {
            boolean idsOnly = toBool(pp.getIdsOnly());
            ret.objectInfo = !(toBool(pp.getSkipInfo()) || idsOnly);
            ret.objectData = !(toBool(pp.getSkipData()) || idsOnly);
            ret.objectKeys = !(toBool(pp.getSkipKeys()) || idsOnly);
        }
        return ret;
    }
    
    private kbasesearchengine.ObjectData fromSearch(
            final kbasesearchengine.search.ObjectData od) {
        final kbasesearchengine.ObjectData ret = new kbasesearchengine.ObjectData();
        ret.withGuid(od.getGUID().toString());
        ret.withObjectProps(new HashMap<>());
        if (od.getParentGUID().isPresent()) {
            ret.withParentGuid(od.getParentGUID().get().toString());
        }
        if (od.getTimestamp().isPresent()) {
            ret.withTimestamp(od.getTimestamp().get().toEpochMilli());
        }
        if (od.getData().isPresent()) {
            ret.withData(new UObject(od.getData().get()));
        }
        if (od.getParentData().isPresent()) {
            ret.withParentData(new UObject(od.getParentData().get()));
        }
        ret.withObjectName(od.getObjectName().orNull());
        ret.withKeyProps(od.getKeyProperties());
        addObjectProp(ret, od.getCreator().orNull(), "creator");
        addObjectProp(ret, od.getCopier().orNull(), "copied");
        addObjectProp(ret, od.getModule().orNull(), "module");
        addObjectProp(ret, od.getMethod().orNull(), "method");
        addObjectProp(ret, od.getModuleVersion().orNull(), "module_ver");
        addObjectProp(ret, od.getCommitHash().orNull(), "commmit");
        return ret;
    }
    
    private void addObjectProp(final ObjectData ret, final String prop, final String propkey) {
        if (prop != null) {
            ret.getObjectProps().put(propkey, prop);
        }
    }

    public SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception {
        long t1 = System.currentTimeMillis();
        kbasesearchengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        kbasesearchengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        Map<String, Integer> ret = indexingStorage.searchTypes(matchFilter, accessFilter);
        return new SearchTypesOutput().withTypeToCount(ret.keySet().stream().collect(
                Collectors.toMap(Function.identity(), c -> (long)(int)ret.get(c))))
                .withSearchTime(System.currentTimeMillis() - t1);
    }
    
    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user) 
            throws Exception {
        long t1 = System.currentTimeMillis();
        kbasesearchengine.search.MatchFilter matchFilter = toSearch(params.getMatchFilter());
        List<kbasesearchengine.search.SortingRule> sorting = null;
        if (params.getSortingRules() != null) {
            sorting = params.getSortingRules().stream().map(this::toSearch).collect(
                    Collectors.toList());
        }
        kbasesearchengine.search.AccessFilter accessFilter = toSearch(params.getAccessFilter(),
                user);
        kbasesearchengine.search.Pagination pagination = toSearch(params.getPagination());
        kbasesearchengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        //TODO NNOW exludeSubObjects from input
        FoundHits hits = indexingStorage.searchObjects(params.getObjectType(),
                matchFilter, sorting, accessFilter, pagination, postProcessing, false);
        SearchObjectsOutput ret = new SearchObjectsOutput();
        ret.withPagination(fromSearch(hits.pagination));
        ret.withSortingRules(hits.sortingRules.stream().map(this::fromSearch).collect(
                Collectors.toList()));
        if (hits.objects == null) {
            ret.withObjects(hits.guids.stream().map(guid -> new kbasesearchengine.ObjectData().
                    withGuid(guid.toString())).collect(Collectors.toList()));
        } else {
            ret.withObjects(hits.objects.stream().map(this::fromSearch).collect(
                    Collectors.toList()));
        }
        ret.withTotal((long)hits.total);
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }

    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        final long t1 = System.currentTimeMillis();
        final Set<Integer> accessGroupIDs =
                new HashSet<>(accessGroupProvider.findAccessGroupIds(user));
        final Set<GUID> guids = new LinkedHashSet<>();
        for (final String guid : params.getGuids()) {
            final GUID g = new GUID(guid);
            //TODO DP this is a quick fix for now, doesn't take data palettes into account
            if (accessGroupIDs.contains(g.getAccessGroupId())) {
                // don't throw an error, just don't return data
                guids.add(g);
            }
        }
        final kbasesearchengine.search.PostProcessing postProcessing = 
                toSearch(params.getPostProcessing());
        final List<kbasesearchengine.search.ObjectData> objs = indexingStorage.getObjectsByIds(
                guids, postProcessing);
        final GetObjectsOutput ret = new GetObjectsOutput().withObjects(objs.stream()
                .map(this::fromSearch).collect(Collectors.toList()));
        ret.withSearchTime(System.currentTimeMillis() - t1);
        return ret;
    }
    
    public Map<String, TypeDescriptor> listTypes(String uniqueType) throws Exception {
        //TODO VERS remove keys from TypeDescriptor, document that listObjectTypes only returns the most recent version of each type
        Map<String, TypeDescriptor> ret = new LinkedHashMap<>();
        for (ObjectTypeParsingRules otpr : typeStorage.listObjectTypeParsingRules()) {
            String typeName = otpr.getGlobalObjectType().getType();
            if (uniqueType != null && !uniqueType.equals(typeName)) {
                continue;
            }
            String uiTypeName = otpr.getUiTypeName();
            if (uiTypeName == null) {
                uiTypeName = guessUIName(typeName);
            }
            List<KeyDescription> keys = new ArrayList<>();
            for (IndexingRules ir : otpr.getIndexingRules()) {
                if (ir.isNotIndexed()) {
                    continue;
                }
                String keyName = ir.getKeyName();
                String uiKeyName = ir.getUiName();
                String keyValueType = ir.getKeywordType().orNull();
                if (keyValueType == null) {
                    keyValueType = "string"; //TODO CODE this seems wrong for fulltext, which is the only case where keyWordtype is null
                }
                long hidden = ir.isUiHidden() ? 1L : 0L;
                KeyDescription kd = new KeyDescription().withKeyName(keyName)
                        .withKeyUiTitle(uiKeyName).withKeyValueType(keyValueType)
                        .withKeyValueType(keyValueType).withHidden(hidden)
                        .withLinkKey(ir.getUiLinkKey().orNull());
                keys.add(kd);
            }
            TypeDescriptor td = new TypeDescriptor().withTypeName(typeName)
                    .withTypeUiTitle(uiTypeName).withKeys(keys);
            ret.put(typeName, td);
        }
        return ret;
    }

    private static String guessUIName(String id) {
        return id.substring(0, 1).toUpperCase() + id.substring(1);
    }
    
}
