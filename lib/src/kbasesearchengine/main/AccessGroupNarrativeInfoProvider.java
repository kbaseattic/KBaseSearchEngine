package kbasesearchengine.main;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.UObject;
import us.kbase.workspace.ListWorkspaceIDsParams;
import us.kbase.workspace.ListWorkspaceIDsResults;
import us.kbase.workspace.GetObjectInfo3Results;
import us.kbase.workspace.WorkspaceClient;

/** A provider for KBase workspace, object and narrative info.
 * @author gaprice@lbl.gov
 * @author ganapathy@bnl.gov
 */
public class AccessGroupNarrativeInfoProvider implements NarrativeInfoProvider,
        AccessGroupInfoProvider, ObjectInfoProvider {

    //TODO TEST

    /* wsHandler created with an event handler pointing at the workspace from which data should
     * be retrieved. This should be the same workspace as that from which the data is indexed.
     */

    private final WorkspaceEventHandler wsHandler;

    /** Create the provider.
     * @param wsHandler a workspace event handler, with workspace client initialized
     *                  with administrator credentials.
     */
    public AccessGroupNarrativeInfoProvider(final WorkspaceEventHandler wsHandler) {
        Utils.nonNull(wsHandler, "WorkspaceHandler");
        this.wsHandler = wsHandler;
    }

    /** For the given workspace ID, returns workspace info related to the narrative.
     * @param accessGroupId accessGroup id.
     */
    @Override
    public NarrativeInfo findNarrativeInfo(final Long accessGroupId) {
        Tuple9 <Long, String, String, String, Long, String, String,
                String, Map<String,String>> wsInfo;

        try {
            wsInfo = wsHandler.getWorkspaceInfo(accessGroupId);
        }
        catch (IOException | JsonClientException e) {
            System.out.println("ERROR: Failed retrieving workspace info: " + e.getMessage());
            wsInfo = null;
        }

        if (wsInfo == null) return null;

        final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());

        final NarrativeInfo tempNarrInfo =
                new NarrativeInfo(null, null, timeMilli, wsInfo.getE3());

        final Map<String, String> wsInfoMeta = wsInfo.getE9();

        if (wsInfoMeta.containsKey("narrative") &&
                wsInfoMeta.containsKey("narrative_nice_name")) {
            tempNarrInfo.withNarrativeName(wsInfoMeta.get("narrative_nice_name"))
                    .withNarrativeId(Long.parseLong(wsInfoMeta.get("narrative")));
        }
        return tempNarrInfo;
    }

    /** Get the workspace information for an object from the workspace service
     * to which this handler is communicating.
     * @param list of object refs: workspaceId/objectId/verId.
     * @return a map of object ref and object info (Tuple11<>) as returned from the workspace API.
    */@Override
    public final Map<String, Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>>> getObjectsInfo(
            final Iterable <? extends String> objRefs) {

        Set<String> objectRefs = new HashSet<>();
        for (final String objRef: objRefs) {
            objectRefs.add(objRef);
        }
        final Map<String, Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> retVal = new HashMap<>();
        GetObjectInfo3Results getObjInfo3Results;
        try {
            getObjInfo3Results = wsHandler.getObjectsInfo(objectRefs);
        }
        catch (IOException | JsonClientException e) {
            System.out.println("ERROR: Failed retrieving objects info: " + e.getMessage());
            return retVal;
        }

        int index = 0;
        final List<List<String>> resultsPaths = getObjInfo3Results.getPaths();
        for (final List<String> path: resultsPaths) {
            if (path != null) {
                retVal.put(path.get(0), getObjInfo3Results.getInfos().get(index));
            }
            index = index + 1;
        }

        // if the objInfo for an input ref is not returned, assign a null value for that ref key
        for (final String objectRef: objectRefs) {
            if (!retVal.containsKey(objectRef)) {
               retVal.put(objectRef, null);
            }
        }
        return retVal;
    }

    /** Get the workspace information for an object from the workspace service
     * to which this handler is communicating.
     * @param an object ref: workspaceId/objectId/verId.
     * @return object info (Tuple11<>) as returned from the workspace API.
     */
    @Override
    public final Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>> getObjectInfo(
            final  String objectRef) {

        return getObjectsInfo(ImmutableSet.of(objectRef)).get(objectRef);
    }

    /** Get the workspace information for a workspace from the workspace service to which this
     * handler is communicating.
     * @param accessGroupId the integer ID of the workspace.
     * @return the workspace info as returned from the workspace.
     */
    @Override
    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> getAccessGroupInfo(
            final Long accessGroupId) {

        try {
            return wsHandler.getWorkspaceInfo(accessGroupId);
        }
        catch (IOException | JsonClientException e) {
            System.out.println("ERROR: Failed retrieving workspace info: " + e.getMessage());
            return null;
        }
    }
}
