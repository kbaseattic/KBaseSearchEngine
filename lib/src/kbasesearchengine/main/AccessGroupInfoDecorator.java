package kbasesearchengine.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final String removedGuids = "removedGuids";
    private final String removedGuidsEnv = "KBASE_SEARCH_SHOW_REMOVED_GUIDS";

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
        SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        if (searchObjsOutput.getAdditionalProperties().get(removedGuids) == null &&
                "true".equals(System.getProperty(removedGuidsEnv))){
            searchObjsOutput.getAdditionalProperties().put(removedGuids, new ArrayList<>());
        }
        final List<String> inputArr = "true".equals(System.getProperty(removedGuidsEnv)) ?
                (List<String>) searchObjsOutput.getAdditionalProperties().get(removedGuids) : new ArrayList<>();

        if (Objects.nonNull(params.getPostProcessing())) {
            if (Objects.nonNull(params.getPostProcessing().getAddAccessGroupInfo()) &&
                    params.getPostProcessing().getAddAccessGroupInfo() == 1) {
                searchObjsOutput = searchObjsOutput
                        .withAccessGroupsInfo(addAccessGroupsInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getAccessGroupsInfo(),
                                inputArr))
                        .withObjectsInfo(addObjectsInfo(
                                searchObjsOutput.getObjects(),
                                searchObjsOutput.getObjectsInfo(),
                                inputArr));
            }
        }
        return searchObjsOutput;
    }

    @Override
    public GetObjectsOutput getObjects(final GetObjectsInput params, final String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        if (getObjsOutput.getAdditionalProperties().get(removedGuids) == null &&
                "true".equals(System.getProperty(removedGuidsEnv))){
            getObjsOutput.getAdditionalProperties().put(removedGuids, new ArrayList<>());
        }
        final List<String> inputArr = "true".equals(System.getProperty(removedGuidsEnv)) ?
                (List<String>) getObjsOutput.getAdditionalProperties().get(removedGuids) : new ArrayList<>();
        if (Objects.nonNull(params.getPostProcessing())) {
            if (Objects.nonNull(params.getPostProcessing().getAddAccessGroupInfo()) &&
                    params.getPostProcessing().getAddAccessGroupInfo() == 1) {
                getObjsOutput = getObjsOutput
                        .withAccessGroupsInfo(addAccessGroupsInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getAccessGroupsInfo(),
                                inputArr))
                        .withObjectsInfo(addObjectsInfo(
                                getObjsOutput.getObjects(),
                                getObjsOutput.getObjectsInfo(),
                                inputArr));
            }
        }
        return getObjsOutput;
    }

    private Map<Long, Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>>> addAccessGroupsInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple9<Long, String, String, String, Long, String,
                    String, String, Map<String, String>>> accessGroupInfoMap,
            final List<String> removedGuids)
            throws IOException, JsonClientException {

        final Map<Long, Tuple9<Long, String, String, String, Long, String,
                String, String, Map<String, String>>> retVal = new HashMap<>();

        if (Objects.nonNull(accessGroupInfoMap)) {
            retVal.putAll(accessGroupInfoMap);
        }

        final Iterator<ObjectData> iter = objects.iterator();
        while(iter.hasNext()){
            final ObjectData objData = iter.next();
            final GUID guid = new GUID(objData.getGuid());
            if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode())) {
                final long workspaceId = guid.getAccessGroupId();
                final Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>> tempWorkspaceInfo =
                        wsInfoProvider.getWorkspaceInfo(workspaceId);

                   if(tempWorkspaceInfo.getE6().equals("n") && tempWorkspaceInfo.getE7().equals("n") ){
                        iter.remove();
                        removedGuids.add(objData.getGuid());
                    }else{
                        retVal.put(workspaceId, tempWorkspaceInfo);
                    }
            } else{
                iter.remove();
                removedGuids.add(objData.getGuid());
            }
        }

        if (removedGuids.size() > 0) {
            getLogger().info("inaccessible guids: {}", removedGuids);
        }

        return retVal;
    }

    private Map<String, Tuple11<Long, String, String, String,
            Long, String, Long, String, String, Long, Map<String, String>>> addObjectsInfo(
            final List<ObjectData> objects,
            final Map<String, Tuple11<Long, String, String, String,
            Long, String, Long, String, String, Long, Map<String, String>>> objectInfoMap,
            final List<String> removedGuids)
            throws IOException, JsonClientException {

        final Map<String, Tuple11<Long, String, String, String,
                Long, String, Long, String, String, Long, Map<String, String>>> retVal = new HashMap<>();

        if (Objects.nonNull(objectInfoMap)) {
            retVal.putAll(objectInfoMap);
        }

        final List<String> objRefs = objects.stream().map( obj -> obj.getGuid()).collect(Collectors.toList());


        final Map<String, Tuple11<Long, String, String, String, Long,
                String, Long, String, String, Long, Map<String, String>>> objsInfo =
                objInfoProvider.getObjectsInfo(objRefs);

        final  Map<String, Tuple11<Long, String, String, String, Long,
                String, Long, String, String, Long, Map<String, String>>> filteredObjsInfo = new HashMap<>();

        if (objsInfo != null){
            final Iterator<ObjectData> iter = objects.iterator();
            while(iter.hasNext()){
                final ObjectData objData = iter.next();
                final String guidStr = objData.getGuid();
                final GUID guid = new GUID(guidStr);

                if (WorkspaceEventHandler.STORAGE_CODE.equals(guid.getStorageCode()) &&
                        objsInfo.containsKey(guidStr)) {
                    filteredObjsInfo.put(guidStr, objsInfo.get(guidStr));

                //not a narrative workspace or inaccessible
                } else{
                    iter.remove();
                    removedGuids.add(objData.getGuid());
                }
            }
            retVal.putAll(filteredObjsInfo);
        }
        return retVal;
    }
    
    private Logger getLogger() {
        return LoggerFactory.getLogger(NarrativeInfoDecorator.class);
    }
}
