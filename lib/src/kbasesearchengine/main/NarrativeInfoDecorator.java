package kbasesearchengine.main;

import java.io.IOException;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import us.kbase.common.service.Tuple5;
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
    private final static String REMOVED_GUIDS = "removed_guids";
    private final static String REMOVED_GUIDS_ENV = "KBASE_SEARCH_SHOW_REMOVED_GUIDS";

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
        SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        final List<String> inputArr = new ArrayList<>();
        if ("true".equals(System.getenv(REMOVED_GUIDS_ENV))){
            searchObjsOutput.getAdditionalProperties().put(REMOVED_GUIDS, inputArr);
        }
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddNarrativeInfo() != null &&
                    params.getPostProcessing().getAddNarrativeInfo() == 1) {
                searchObjsOutput = searchObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                        searchObjsOutput.getObjects(),
                        searchObjsOutput.getAccessGroupNarrativeInfo(),
                        inputArr));
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
        final List<String> inputArr = new ArrayList<>();
        if ("true".equals(System.getenv(REMOVED_GUIDS_ENV))){
            getObjsOutput.getAdditionalProperties().put(REMOVED_GUIDS, inputArr);
        }
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddNarrativeInfo() != null &&
                    params.getPostProcessing().getAddNarrativeInfo() == 1) {
                getObjsOutput = getObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                        getObjsOutput.getObjects(),
                        getObjsOutput.getAccessGroupNarrativeInfo(),
                        inputArr));
            }
        }
        return getObjsOutput;
    }

    /**
     * Adds narrative information for non deleted workspaces. Removes results otherwise and
     * log list of removed guids. If env "KBASE_SEARCH_SHOW_REMOVED_GUIDS", is set to true, list of removed guids
     * is added to additionalProperties.
     */
    private Map<Long, Tuple5 <String, Long, Long, String, String>> addNarrativeInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrInfo,
            final List<String> removedGuids) {

        final Map<Long, Tuple5 <String, Long, Long, String, String>> retVal = new HashMap<>();

        if (accessGroupNarrInfo != null) {
            retVal.putAll(accessGroupNarrInfo);
        }
        final Set<String> userNames = new HashSet<>();
        final Iterator<ObjectData> iter = objects.iterator();
        final Set<Long> seenWorkspaces = new HashSet<>();
        while (iter.hasNext()) {
            final ObjectData objData = iter.next();
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                final long workspaceId = guid.getAccessGroupId();

                if(seenWorkspaces.contains(workspaceId)){
                    continue;
                }

                seenWorkspaces.add(workspaceId);
                final NarrativeInfo narrInfo = narrInfoProvider.findNarrativeInfo(workspaceId);

                //provider sets narrative info to null for any workspace errors. 
                if (narrInfo != null) {
                    final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                            new Tuple5<String, Long, Long, String, String>()
                                    .withE1(narrInfo.getNarrativeName())
                                    .withE2(narrInfo.getNarrativeId())
                                    .withE3(narrInfo.getTimeLastSaved())
                                    .withE4(narrInfo.getWsOwnerUsername());
                    userNames.add(tempNarrInfo.getE4());
                    retVal.put(workspaceId, tempNarrInfo);
                }
                else {
                    iter.remove();
                    removedGuids.add(objData.getGuid());

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
            getLogger().error("ERROR: Failed retrieving workspace owner realname(s): " +
                            "setting to null: {}", e.getMessage());
        }

        if (removedGuids.size() > 0) {
            getLogger().info("inaccessible guids: {}", removedGuids);
        }
        return retVal;
    }

    private Logger getLogger() {
        return LoggerFactory.getLogger(NarrativeInfoDecorator.class);
    }
}
