package kbasesearchengine.main;

import kbasesearchengine.ObjectData;
import kbasesearchengine.common.GUID;
import kbasesearchengine.tools.Utils;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Uma Ganapathy
 * @author gaprice@lbl.gov
 *
 */
public class NarrativeInfoDecorator implements SearchInterface {

    private final WorkspaceEventHandler weh;
    private final SearchInterface searchInterface;

    public NarrativeInfoDecorator(
            final SearchInterface searchInterface,
            final WorkspaceEventHandler wsHandler) {
        Utils.nonNull(searchInterface, "searchInterface");
        Utils.nonNull(wsHandler, "wsHandler");
        this.searchInterface = searchInterface;
        this.weh = wsHandler;
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
            throws IOException, JsonClientException {

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

            final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());
            final Tuple5<String, Long, Long, String, String> tempNarrInfo =
                        new Tuple5<String, Long, Long, String, String>()
                    .withE3(timeMilli)      // modification time
                    .withE4(wsInfo.getE3()) // workspace user name
                    .withE5(null);          // TODO workspace user, real name

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
