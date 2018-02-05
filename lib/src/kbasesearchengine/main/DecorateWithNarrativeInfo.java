package kbasesearchengine.main;

import kbasesearchengine.*;
import kbasesearchengine.ObjectData;
import kbasesearchengine.common.GUID;
import us.kbase.common.service.JsonClientException;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;
import workspace.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by umaganapathy on 1/26/18.
 */
public class DecorateWithNarrativeInfo extends ObjectDecorator {

    /** The storage code for workspace events. */
    public static final String STORAGE_CODE = "WS";
    private final WorkspaceClient wsClient;

    public DecorateWithNarrativeInfo(SearchInterface searchInterface,
                                     WorkspaceClient wsClient) {
        super(searchInterface);
        this.wsClient = wsClient;
    }

    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user)
            throws Exception {
        SearchObjectsOutput searchObjsOutput = super.searchObjects(params, user);
        searchObjsOutput.setAccessGroupNarrativeInfo(addNarrativeInfo(searchObjsOutput.getObjects(),
                searchObjsOutput.getAccessGroupNarrativeInfo()));
        return searchObjsOutput;
    }

    public GetObjectsOutput getObjects(GetObjectsInput params, String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = super.getObjects(params, user);
        getObjsOutput.setAccessGroupNarrativeInfo(addNarrativeInfo(getObjsOutput.getObjects(),
                getObjsOutput.getAccessGroupNarrativeInfo()));
        return getObjsOutput;
    }

    private Map<Long, Tuple5 <String, Long, String, String, String>> addNarrativeInfo(
            List<ObjectData> objects,
            Map<Long, Tuple5 <String, Long, String, String, String>> accessGroupNarrInfo)
                throws IOException, JsonClientException {

        long wsId;
        Tuple9 <Long, String, String, String, Long, String, String,
                String,Map<String,String>> wsInfo;
        Tuple5 <String, Long, String, String, String> tempNarrInfo =
                new Tuple5<>();
        final Map<Long, Tuple5 <String, Long, String, String, String>> retVal = new HashMap<>();

        if (accessGroupNarrInfo != null) {
            retVal.putAll(accessGroupNarrInfo);
        }

        List<Long> wsIdsList = new ArrayList<Long>();

        for (ObjectData objData: objects) {

            GUID guid = new GUID(objData.getGuid());

            String storageCode = guid.getStorageCode();

            if (STORAGE_CODE.equals(storageCode)) {
                wsId = guid.getAccessGroupId();
                if (!wsIdsList.contains(wsId))
                    wsIdsList.add(wsId);
            }
            else {
                // TODO: handle other storage types?
            }
        }
        for (long workspaceId: wsIdsList) {
            if (!retVal.containsKey(workspaceId)) {
                // get workspace info meta data
                WorkspaceEventHandler weh = new WorkspaceEventHandler(
                                                new CloneableWorkspaceClientImpl(wsClient));
                wsInfo = weh.getWorkspaceInfo(workspaceId);
                tempNarrInfo.setE3(wsInfo.getE4());
                tempNarrInfo.setE4(wsInfo.getE3());
                tempNarrInfo.setE5("TBD");

                Map<String, String> wsInfoMeta = wsInfo.getE9();

                if ( wsInfoMeta.containsKey("narrative") &&
                        wsInfoMeta.containsKey("narrative_nice_name") ) {
                    tempNarrInfo.setE1(wsInfoMeta.get("narrative_nice_name"));
                    tempNarrInfo.setE2(Long.parseLong(wsInfoMeta.get("narrative")));
                }
                else { // will get here only when testing locally
                    tempNarrInfo.setE1("Not Available");
                    tempNarrInfo.setE2((long)-1);
                }
                retVal.put(workspaceId, tempNarrInfo);
            }
        }
        return retVal;
    }
}
