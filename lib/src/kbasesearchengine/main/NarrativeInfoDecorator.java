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
import kbasesearchengine.authorization.TemporaryAuth2Client;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;
import kbasesearchengine.common.GUID;
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
    private final TemporaryAuth2Client authClient;
    private final String token;

    /** Create a decorator.
     * @param searchInterface the search interface to decorate. This may be a root interface that
     * produces data from a search storage system or another decorator.
     * @param wsHandler a workspace event handler pointing at the workspace from which
     * data should be retrieved. This should be the same workspace as that from which the data
     * is indexed.
     * @param authClient a client for the KBase authentication system.
     * @param token a token to be used with the auth client. The token needs no particular
     * privileges.
     */
    public NarrativeInfoDecorator(
            final SearchInterface searchInterface,
            final WorkspaceEventHandler wsHandler,
            final TemporaryAuth2Client authClient,
            final String token) {
        Utils.nonNull(searchInterface, "searchInterface");
        Utils.nonNull(wsHandler, "wsHandler");
        Utils.nonNull(authClient, "authClient");
        Utils.notNullOrEmpty(token, "token cannot be null or whitespace only");
        this.searchInterface = searchInterface;
        this.weh = wsHandler;
        this.authClient = authClient;
        this.token = token;
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
            throws IOException, JsonClientException, Auth2Exception {

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
        final Set<String> userNames = new HashSet<>();
        for (final long workspaceId: wsIdsSet) {
            final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                    getNarrativeInfo(workspaceId);
            userNames.add(tempNarrInfo.getE4());
            retVal.put(workspaceId, tempNarrInfo);
        }
        final Map<String, String> displayNames = authClient.getUserDisplayNames(token, userNames);
        // e5 is the full / display name, e4 is the user name
        // defaults to the existing name so names from previous decorator layers aren't set to null
        retVal.values().stream().forEach(t -> t.withE5(
                displayNames.getOrDefault(t.getE4(), t.getE5())));
        return retVal;
    }
    
    private Tuple5<String, Long, Long, String, String> getNarrativeInfo(final long wsid)
            throws IOException, JsonClientException {
        final Tuple9 <Long, String, String, String, Long, String, String,
                String, Map<String,String>> wsInfo;

        try {
            wsInfo = weh.getWorkspaceInfo(wsid);
        } catch (IOException e) {
            throw new IOException("Failed retrieving workspace info: " + e.getMessage(), e);
        } catch (JsonClientException e) {
            throw new JsonClientException("Failed retrieving workspace info: "
                    + e.getMessage(), e);
        }
        
        final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());
        final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                    new Tuple5<String, Long, Long, String, String>()
                .withE3(timeMilli)      // modification time
                .withE4(wsInfo.getE3()); // workspace user name
                // e5 gets filled in later
        
        final Map<String, String> wsInfoMeta = wsInfo.getE9();
        
        if (wsInfoMeta.containsKey("narrative") &&
                wsInfoMeta.containsKey("narrative_nice_name")) {
            tempNarrInfo.withE1(wsInfoMeta.get("narrative_nice_name"))
                    .withE2(Long.parseLong(wsInfoMeta.get("narrative")));
        }
        return tempNarrInfo;
    }
}
