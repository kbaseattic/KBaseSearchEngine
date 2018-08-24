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
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.JsonClientException;
import org.slf4j.LoggerFactory;

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
        SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        if(searchObjsOutput.getRemovedGuids() == null){
            searchObjsOutput.setRemovedGuids(new ArrayList<>());
        }
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddNarrativeInfo() != null &&
                    params.getPostProcessing().getAddNarrativeInfo() == 1) {
                searchObjsOutput = searchObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                        searchObjsOutput.getObjects(),
                        searchObjsOutput.getAccessGroupNarrativeInfo(),
                        searchObjsOutput.getRemovedGuids()));
            }
        }
        return searchObjsOutput;
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
                        getObjsOutput.getRemovedGuids()));
            }
        }
        return getObjsOutput;
    }

    /**
     * Adds narrative information for non deleted and valid narrative workspaces. Removes results otherwise and
     * add the removed result's guid to removedGuids list.
     * @param objects
     * @param accessGroupNarrInfo
     * @param removedGuids
     * @return
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
        while(iter.hasNext()) {
            final ObjectData objData = iter.next();
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                final long workspaceId = guid.getAccessGroupId();
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
                }
                else {
                    iter.remove();
                    removedGuids.add(objData.getGuid());
                    retVal.put(workspaceId, null);
                }
            //if workspace is not a narrative, remove results from search
            } else{
                iter.remove();
                removedGuids.add(objData.getGuid());
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
