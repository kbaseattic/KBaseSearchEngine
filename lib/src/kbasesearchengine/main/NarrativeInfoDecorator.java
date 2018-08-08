package kbasesearchengine.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.ObjectData;
import kbasesearchengine.Pagination;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.authorization.AuthInfoProvider;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.JsonClientException;
import org.slf4j.LoggerFactory;
import us.kbase.common.service.Tuple9;

/**
 * Decorates the results from a {@link SearchInterface} with information about workspaces that
 * contain objects in the search results. See
 * {@link SearchObjectsOutput#getAccessGroupNarrativeInfo()} and
 * {@link GetObjectsOutput#getAccessGroupNarrativeInfo()}, which is where the information is
 * stored.
 * 
 * If a previous decorator or the root search implementation itself provides information in the
 * narrative information, it will be overwritten if any workspace IDs in the {@link ObjectData}
 * match with the IDs in the narrative information.
 * 
 * @author Uma Ganapathy
 * @author gaprice@lbl.gov
 *
 */
public class NarrativeInfoDecorator implements SearchInterface {

    private final SearchInterface searchInterface;
    private final NarrativeInfoProvider narrInfoProvider;
    private final AuthInfoProvider authInfoProvider;

    /** Create a decorator.
     * @param searchInterface the search interface to decorate. This may be a root interface that
     * produces data from a search storage system or another decorator.
     * @param narrInfoProvider a cache with workspace info with a mapping of <wsId, NarrativeInfo>
     * NarrativeInfo obtained with an event handler pointing at the workspace from which data
     * should be retrieved. This should be the same workspace as that from which the data is indexed.
     * @param authInfoProvider a cache with data from auth with a mapping of <UserName, DisplayName>
     */
    public NarrativeInfoDecorator(
            final SearchInterface searchInterface,
            final NarrativeInfoProvider narrInfoProvider,
            final AuthInfoProvider authInfoProvider) {

        Utils.nonNull(searchInterface, "SearchInterface");
        Utils.nonNull(narrInfoProvider, "NarrativeInfoProvider");
        Utils.nonNull(authInfoProvider, "AuthInfoProvider");
        this.searchInterface = searchInterface;
        this.narrInfoProvider = narrInfoProvider;
        this.authInfoProvider = authInfoProvider;
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

            if (params.getPostProcessing() != null && params.getPostProcessing().getAddNarrativeInfo() != null
                    && params.getPostProcessing().getAddNarrativeInfo() == 1) {

                Long curTotalObs = (searchObjsOutput == null) ? 0L : searchObjsOutput.getObjects().size();

                SearchObjectsOutput modifiedSearchRes = searchRes.withAccessGroupNarrativeInfo(addNarrativeInfo(
                        searchRes.getObjects(),
                        searchRes.getAccessGroupNarrativeInfo()
                        ,curTotalObs, count));

                searchObjsOutput = combineWithOtherSearchObjectsOuput(searchObjsOutput, modifiedSearchRes,
                        searchRes.getObjects().size() -  modifiedSearchRes.getObjects().size());

                if(searchRes.getTotalInPage() < count){
                    return searchObjsOutput;
                }
            }else{
                return searchRes;
            }


        }

        return searchObjsOutput;
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
            Map<Long, Tuple5 <String, Long, Long, String, String>> accessGrpNarInfo;
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

    @Override
    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddNarrativeInfo() != null &&
                    params.getPostProcessing().getAddNarrativeInfo() == 1) {
                getObjsOutput = getObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                        getObjsOutput.getObjects(),
                        getObjsOutput.getAccessGroupNarrativeInfo(),
                        0L, Long.MAX_VALUE));
            }
        }
        return getObjsOutput;
    }

    private Map<Long, Tuple5 <String, Long, Long, String, String>> addNarrativeInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrInfo,
            Long curTotal, final Long targetTotal) {

        final Map<Long, Tuple5 <String, Long, Long, String, String>> retVal = new HashMap<>();

        if (accessGroupNarrInfo != null) {
            retVal.putAll(accessGroupNarrInfo);
        }
        final Set<Long> wsIdsSet = new HashSet<>();
        final Set<Long> deletedWsIdsSet = new HashSet<>();
        final Set<String> userNames = new HashSet<>();


        final Iterator<ObjectData> iter = objects.iterator();

        while(iter.hasNext()){
            final ObjectData objData = iter.next();
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                final long workspaceId = (long) guid.getAccessGroupId();

                if(curTotal == targetTotal){
                    //remove extra results
                    do{
                        iter.remove();
                    }while(iter.hasNext());
                    break;
                }if(wsIdsSet.contains(workspaceId)){
                    curTotal = curTotal + 1L;
                    continue;
                }else if(deletedWsIdsSet.contains(workspaceId)){
                    iter.remove();
                    continue;
                }

                final NarrativeInfo narrInfo = narrInfoProvider.findNarrativeInfo(workspaceId);
                if (narrInfo != null) {
                    final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                            new Tuple5<String, Long, Long, String, String>()
                                    .withE1(narrInfo.getNarrativeName())
                                    .withE2(narrInfo.getNarrativeId())
                                    .withE3(narrInfo.getTimeLastSaved())
                                    .withE4(narrInfo.getWsOwnerUsername());
                    userNames.add(tempNarrInfo.getE4());
                    retVal.put(workspaceId, tempNarrInfo);
                    wsIdsSet.add(workspaceId);
                    curTotal = curTotal + 1L;
                } else{
                    deletedWsIdsSet.add(workspaceId);
                    iter.remove();
                }
            }

        }
        try {
            final Map<String, String> displayNames = authInfoProvider.findUserDisplayNames(userNames);
            // e5 is the full / display name, e4 is the user name
            // defaults to the existing name so names from previous decorator layers aren't set to null
            for (Map.Entry<Long, Tuple5<String, Long, Long, String, String>> entry : retVal.entrySet()) {
                Tuple5<String, Long, Long, String, String> value = entry.getValue();
                if (value != null) {
                    value.withE5(displayNames.getOrDefault(value.getE4(), value.getE5()));
                }
            }
        }
        catch (IOException | Auth2Exception e) {
            LoggerFactory.getLogger(getClass()).error("ERROR: Failed retrieving workspace owner realname(s): " +
                            "setting to null: {}", e.getMessage());
        }
        return retVal;
    }
}
