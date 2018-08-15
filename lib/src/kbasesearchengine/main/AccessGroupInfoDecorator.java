package kbasesearchengine.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;
import us.kbase.workspace.GetObjectInfo3Results;
import com.google.common.base.Optional;
import us.kbase.common.service.JsonClientException;

/**
 * Decorates the results from a {@link SearchInterface} with information about workspaces
 * and objects in the search results. See
 * {@link SearchObjectsOutput#getWorkspacesInfo()} and
 * {@link GetObjectsOutput#getObjectsInfo()}, which is where the information is
 * stored.
 *
 * If a previous decorator or the root search implementation itself provides information in the
 * narrative information, it will be overwritten if any workspace IDs in the {@link ObjectData}
 * match with the IDs in the narrative information.
 *
 * @author Uma Ganapathy
 *
 */
public class AccessGroupInfoDecorator implements SearchInterface {

    private final WorkspaceInfoProvider wsInfoProvider;
    private final ObjectInfoProvider objInfoProvider;
    private final SearchInterface searchInterface;

    /** Create a decorator.
     * @param searchInterface the search interface to decorate. This may be a root interface that
     * produces data from a search storage system or another decorator.
     * @param wsInfoProvider a workspace info provider using which the workspace info is retrieved
     *                       from workspace info cache, or from the workspace if not cached.
     *                       The workspace should be the same as that from which the data is indexed.
     * @param objInfoProvider an object info provider using which the objects info is retrieved
     *                       from object info cache, or from the workspace if not cached.
     *                       The workspace should be the same as that from which the data is indexed.
     */
    public AccessGroupInfoDecorator(
            final SearchInterface searchInterface,
            final WorkspaceInfoProvider wsInfoProvider,
            final ObjectInfoProvider objInfoProvider) {
        Utils.nonNull(searchInterface, "searchInterface");
        Utils.nonNull(wsInfoProvider, "wsInfoProvider");
        Utils.nonNull(objInfoProvider, "objInfoProvider");
        this.searchInterface = searchInterface;
        this.wsInfoProvider = wsInfoProvider;
        this.objInfoProvider = objInfoProvider;
    }

    @Override
    public SearchTypesOutput searchTypes(final SearchTypesInput params, final String user)
            throws Exception {
        return searchInterface.searchTypes(params, user);
    }

    @Override
    public Map<String, TypeDescriptor> listTypes(final String uniqueType) throws Exception {
        return searchInterface.listTypes(uniqueType);
    }

    @Override
    public SearchObjectsOutput searchObjects(final SearchObjectsInput params, final String user)
            throws Exception {
        SearchObjectsOutput searchObjsOutput = getEmptySearchObjectsOutput();
        long count = (params.getPagination() == null) ? 50 : params.getPagination().getCount();

        while(searchObjsOutput.getObjects().size() < count){
            final SearchObjectsOutput searchRes = reSearchObjects(params, user, searchObjsOutput);

            if (hasAddNarrativeInfo(params)) {
                Long curTotalObs = (searchObjsOutput == null) ? 0L : searchObjsOutput.getObjects().size();

                SearchObjectsOutput modifiedSearchRes = searchRes
                        .withAccessGroupsInfo(addAccessGroupsInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getAccessGroupsInfo(),
                                curTotalObs,
                                count))
                        .withObjectsInfo(addObjectsInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getObjectsInfo()));

                searchObjsOutput = combineWithOtherSearchObjectsOuput(searchObjsOutput, modifiedSearchRes,
                        searchRes.getObjects().size() -  modifiedSearchRes.getObjects().size());

                //return results if search returns less objects than required by pagination
                if(searchRes.getTotalInPage() < count){
                    return searchObjsOutput;
                }
            }else{
                //return raw search results, if addNarrativeInfo is not set. Note that this may include deleted workspaces.
                return searchRes;
            }

        }

        return searchObjsOutput;
    }

    @Override
    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        if (Objects.nonNull(params.getPostProcessing())) {
            if (Objects.nonNull(params.getPostProcessing().getAddAccessGroupInfo()) &&
                    params.getPostProcessing().getAddAccessGroupInfo() == 1) {
                getObjsOutput = getObjsOutput
                        .withAccessGroupsInfo(addAccessGroupsInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getAccessGroupsInfo(),
                                0L, Long.MAX_VALUE))
                        .withObjectsInfo(addObjectsInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getObjectsInfo()));
            }
        }
        return getObjsOutput;
    }

    private boolean hasAddNarrativeInfo(final SearchObjectsInput params){
        return params.getPostProcessing() != null && params.getPostProcessing().getAddAccessGroupInfo() != null
                && params.getPostProcessing().getAddAccessGroupInfo() == 1;
    }
    private boolean hasAddNarrativeInfo(final GetObjectsInput params){
        return params.getPostProcessing() != null && params.getPostProcessing().getAddAccessGroupInfo() != null
                && params.getPostProcessing().getAddAccessGroupInfo() == 1;
    }

    private SearchObjectsOutput reSearchObjects(final SearchObjectsInput params, final String user, final SearchObjectsOutput prevRes)
            throws Exception {
        SearchObjectsOutput res = getEmptySearchObjectsOutput();
        if(prevRes.getTotal() == 0L){
            res = combineWithOtherSearchObjectsOuput(res, searchInterface.searchObjects(params, user), 0);
        }else{
            final long newStart = prevRes.getPagination().getStart() + prevRes.getTotalInPage();

            Pagination newPag = new Pagination()
                    .withCount(prevRes.getPagination().getCount())
                    .withStart(newStart);
            SearchObjectsInput newParams = new SearchObjectsInput()
                    .withSortingRules(params.getSortingRules())
                    .withPostProcessing(params.getPostProcessing())
                    .withObjectTypes(params.getObjectTypes())
                    .withMatchFilter(params.getMatchFilter())
                    .withAccessFilter(params.getAccessFilter())
                    .withPagination(newPag);

            SearchObjectsOutput temp =  searchInterface.searchObjects(newParams, user);
            res = combineWithOtherSearchObjectsOuput(res, temp, 0);

        }
        return res;
    }

    private Map<Long, Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>>> addAccessGroupsInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>>> accessGroupInfoMap,
            final Long curTotal,
            final Long targetTotal)
            throws IOException, JsonClientException {

        final Map<Long, Tuple9<Long, String, String, String, Long, String,
                String, String, Map<String, String>>> retVal = new HashMap<>();
        long count = curTotal;

        if (Objects.nonNull(accessGroupInfoMap)) {
            retVal.putAll(accessGroupInfoMap);
        }

        final Set<Long> inAccessibleWsIds = new HashSet<>();
        final Iterator<ObjectData> iter = objects.iterator();

        while(iter.hasNext()){
            final ObjectData objData = iter.next();
            final GUID guid = new GUID(objData.getGuid());
            final long workspaceId = (long) guid.getAccessGroupId();

            if(curTotal >= targetTotal){
                //remove extra results
                do{
                    iter.remove();
                }while(iter.hasNext());
                break;
            }

            if(inAccessibleWsIds.contains(workspaceId)){
                iter.remove();
                continue;
            }

            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                final Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>> tempWorkspaceInfo =
                        wsInfoProvider.getWorkspaceInfo(workspaceId);
                if(tempWorkspaceInfo != null){
                    retVal.put(workspaceId, tempWorkspaceInfo);
                    count += 1L;
                }else{
                    inAccessibleWsIds.add(workspaceId);
                    iter.remove();
                }
            }else{
                inAccessibleWsIds.add(workspaceId);
                iter.remove();
            }
        }

        return retVal;
    }

    private Map<String, Tuple11<Long, String, String, String,
            Long, String, Long, String, String, Long, Map<String, String>>> addObjectsInfo(
            final List<ObjectData> objects,
            final Map<String, Tuple11<Long, String, String, String,
            Long, String, Long, String, String, Long, Map<String, String>>> objectInfoMap)
            throws IOException, JsonClientException {

        final Map<String, Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> retVal = new HashMap<>();

        if (Objects.nonNull(objectInfoMap)) {
            retVal.putAll(objectInfoMap);
        }
        final Set<GUID> guidsSet = new HashSet<>();
        for (final ObjectData objData: objects) {
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                guidsSet.add(guid);
            }
        }
        final List<String> objRefs = new ArrayList<>();
        for (final GUID guid: guidsSet) {
            objRefs.add(guid.toRefString());
        }

        final Map<String, Tuple11<Long, String, String, String, Long,
                String, Long, String, String, Long, Map<String, String>>> objsInfo =
                objInfoProvider.getObjectsInfo(objRefs);

        if (objsInfo != null)
            retVal.putAll(objsInfo);
        return retVal;
    }
    private SearchObjectsOutput combineWithOtherSearchObjectsOuput(final SearchObjectsOutput target, final SearchObjectsOutput other, int removedObjs){
        target.setTotalInPage(target.getTotalInPage() + other.getTotalInPage() - (long) removedObjs);
        target.setSearchTime(target.getSearchTime() + other.getSearchTime());

        List<ObjectData> objs= target.getObjects();
        objs.addAll(other.getObjects());
        target.setObjects(objs);

        if(target.getTotal() < other.getTotal()){
            target.setTotal(other.getTotal());
        }

        if(other.getAccessGroupNarrativeInfo() != null){
            Map<Long, Tuple5<String, Long, Long, String, String>> accessGrpNarInfo;
            if(target.getAccessGroupNarrativeInfo() != null){
                accessGrpNarInfo = target.getAccessGroupNarrativeInfo();
            }else{
                accessGrpNarInfo = new HashMap<>();
            }
            accessGrpNarInfo.putAll(other.getAccessGroupNarrativeInfo());
            target.setAccessGroupNarrativeInfo(accessGrpNarInfo);
        }

        if(other.getAccessGroupsInfo() != null){
            Map<Long, Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>>> accessGrpInfo;
            if(target.getAccessGroupsInfo() != null){
                accessGrpInfo = target.getAccessGroupsInfo();
            } else{
                accessGrpInfo = new HashMap<>();
            }
            accessGrpInfo.putAll(other.getAccessGroupsInfo());
            target.setAccessGroupsInfo(accessGrpInfo);
        }

        if(other.getObjectsInfo() != null){
            Map<String, Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> objInfo;
            if(target.getObjectsInfo() != null){
                objInfo = target.getObjectsInfo();
            }else{
                objInfo = new HashMap<>();
            }
            objInfo.putAll(other.getObjectsInfo());
            target.setObjectsInfo(objInfo);
        }


        if(target.getPagination() == null){
            target.setPagination(other.getPagination());
        }

        if(target.getSortingRules() == null){
            target.setSortingRules(other.getSortingRules());
        }

        return target;
    }

    private SearchObjectsOutput getEmptySearchObjectsOutput(){
        return new SearchObjectsOutput()
                .withObjects(new ArrayList<>())
                .withAccessGroupNarrativeInfo(null)
                .withAccessGroupsInfo(null)
                .withObjectsInfo(null)
                .withSearchTime(0L)
                .withTotal(0L)
                .withTotalInPage(0L)
                .withPagination(null)
                .withSortingRules(null);
    }
}
