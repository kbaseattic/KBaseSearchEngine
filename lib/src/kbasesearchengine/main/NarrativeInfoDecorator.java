package kbasesearchengine.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import kbasesearchengine.common.GUID;
import kbasesearchengine.authorization.AuthAPI;

import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple5;
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

    private final WorkspaceEventHandler weh;
    private final SearchInterface searchInterface;
    private final String kbaseIndexerToken;
    private final AuthAPI authAPI;

    /** Create a decorator.
     * @param searchInterface the search interface to decorate. This may be a root interface that
     * produces data from a search storage system or another decorator.
     * @param wsHandler a workspace event handler pointing at the workspace from which
     * data should be retrieved. This should be the same workspace as that from which the data
     * is indexed.
     */
    public NarrativeInfoDecorator(
            final SearchInterface searchInterface,
            final WorkspaceEventHandler wsHandler,
            final String authURLString,
            final String kbaseIndexerToken) {
        Utils.nonNull(searchInterface, "searchInterface");
        Utils.nonNull(wsHandler, "wsHandler");
        this.searchInterface = searchInterface;
        this.weh = wsHandler;
        this.authAPI = new AuthAPI(authURLString);
        this.kbaseIndexerToken = kbaseIndexerToken;
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
        final SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        return searchObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                searchObjsOutput.getObjects(),
                searchObjsOutput.getAccessGroupNarrativeInfo()));
    }

    @Override
    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        final GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        return getObjsOutput.withAccessGroupNarrativeInfo(addNarrativeInfo(
                getObjsOutput.getObjects(),
                getObjsOutput.getAccessGroupNarrativeInfo()));
    }

    private Map<Long, Tuple5 <String, Long, Long, String, String>> addNarrativeInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrInfo)
            throws IOException, JsonClientException, Exception {

        // TODO adding and retreiving from cache
        final Map<Long, Tuple5 <String, Long, Long, String, String>> retVal = new HashMap<>();

        if (accessGroupNarrInfo != null) {
            retVal.putAll(accessGroupNarrInfo);
        }
        final Set<Long> wsIdsSet = new HashSet<>();

        for (final ObjectData objData: objects) {
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                wsIdsSet.add((long) guid.getAccessGroupId());
            }
        }
        for (final long workspaceId: wsIdsSet) {
            final Tuple9 <Long, String, String, String, Long, String, String,
                    String, Map<String,String>> wsInfo;
            
            try {
                wsInfo = weh.getWorkspaceInfo(workspaceId);
            } catch (IOException e) {
                throw new IOException("Failed retrieving workspace info: " + e.getMessage(), e);
            } catch (JsonClientException e) {
                throw new JsonClientException("Failed retrieving workspace info: "
                        + e.getMessage(), e);
            }

            final String wsUsername = wsInfo.getE3();

            final Map<String, Object> displayNameMap = authAPI.getDisplayNames(
                        kbaseIndexerToken,
                        wsUsername);

            // TODO Need to be changed if the API can be used for multiple users
            final Object displayName = displayNameMap.get("display");
            final String wsUserRealname = (displayName == null) ? null : displayName.toString();

            final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());
            final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                        new Tuple5<String, Long, Long, String, String>()
                    .withE3(timeMilli)       // modification time
                    .withE4(wsUsername)      // workspace user name
                    .withE5(wsUserRealname); // workspace user, real name

            final Map<String, String> wsInfoMeta = wsInfo.getE9();

            if (wsInfoMeta.containsKey("narrative") &&
                    wsInfoMeta.containsKey("narrative_nice_name")) {
                tempNarrInfo.withE1(wsInfoMeta.get("narrative_nice_name"))
                        .withE2(Long.parseLong(wsInfoMeta.get("narrative")));
            }
            retVal.put(workspaceId, tempNarrInfo);
        }
        return retVal;
    }
}
