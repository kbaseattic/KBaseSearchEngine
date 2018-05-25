package kbasesearchengine.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
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
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.Tuple11;
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
public class WorkspaceInfoDecorator implements SearchInterface {

    private final WorkspaceEventHandler weh;
    private final SearchInterface searchInterface;

    /** Create a decorator.
     * @param searchInterface the search interface to decorate. This may be a root interface that
     * produces data from a search storage system or another decorator.
     * @param wsHandler a workspace event handler pointing at the workspace from which
     * data should be retrieved. This should be the same workspace as that from which the data
     * is indexed.
     *
     */
    public WorkspaceInfoDecorator(
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
        SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddWorkspaceInfo() != null &&
                    params.getPostProcessing().getAddWorkspaceInfo() == 1) {
                searchObjsOutput = searchObjsOutput
                        .withWorkspacesInfo(addWorkspacesInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getWorkspacesInfo()))
                        .withObjectsInfo(addObjectsInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getObjectsInfo()));
            }
        }
        return searchObjsOutput;
    }

    @Override
    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        if (params.getPostProcessing() != null) {
            if (params.getPostProcessing().getAddWorkspaceInfo() != null &&
                    params.getPostProcessing().getAddWorkspaceInfo() == 1) {
                getObjsOutput = getObjsOutput
                        .withWorkspacesInfo(addWorkspacesInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getWorkspacesInfo()))
                        .withObjectsInfo(addObjectsInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getObjectsInfo()));
            }
        }
        return getObjsOutput;
    }

    private Map<Long, Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>>> addWorkspacesInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>>> workspaceInfoMap)
            throws IOException, JsonClientException {

        final Map<Long, Tuple9<Long, String, String, String, Long, String,
                String, String, Map<String, String>>> retVal = new HashMap<>();

        if (workspaceInfoMap != null) {
            retVal.putAll(workspaceInfoMap);
        }
        final Set<Long> wsIdsSet = new HashSet<>();

        for (final ObjectData objData: objects) {
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                wsIdsSet.add((long) guid.getAccessGroupId());
            }
        }
        for (final long workspaceId: wsIdsSet) {
            final Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>> tempWorkspaceInfo =
                    weh.getWorkspaceInfo(workspaceId);
            retVal.put(workspaceId, tempWorkspaceInfo);
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

        if (objectInfoMap != null) {
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
        if (!objRefs.isEmpty()) {
            final GetObjectInfo3Results getObjInfo3Results = weh.getObjectInfo3(objRefs, 1L, 1L);

            int index = 0;
            final List<List<String>> resultsPaths = getObjInfo3Results.getPaths();
            for (final List<String> path : resultsPaths) {
                if (path != null) {
                    retVal.put(path.get(0), getObjInfo3Results.getInfos().get(index));
                    index = index + 1;
                }
            }
        }
        return retVal;
    }
}
