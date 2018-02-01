package kbasesearchengine.main;

import kbasesearchengine.*;
import kbasesearchengine.ObjectData;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import workspace.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by umaganapathy on 1/26/18.
 */
public class DecorateWithNarrativeInfo extends ObjectDecorator {

    public DecorateWithNarrativeInfo(SearchInterface searchInterface) {
        super(searchInterface);
    }

    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user)
            throws Exception {
        SearchObjectsOutput searchObjsOutput = super.searchObjects(params, user);
        searchObjsOutput.setWsNarrativeInfo(addNarrativeInfo(searchObjsOutput.getObjects(),
                                                             searchObjsOutput.getWsNarrativeInfo()));
        return searchObjsOutput;
    }

    public GetObjectsOutput getObjects(GetObjectsInput params, String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = super.getObjects(params, user);
        getObjsOutput.setWsNarrativeInfo(addNarrativeInfo(getObjsOutput.getObjects(),
                                                          getObjsOutput.getWsNarrativeInfo()));
        return getObjsOutput;
    }

    private Map<String, NarrativeInfo> addNarrativeInfo(List<ObjectData> objects,
                                                 Map<String, NarrativeInfo> wsNarrativeInfo)
                throws RetriableIndexingException, IndexingException {

        long narrativeID;
        String narrativeName;

        for (ObjectData objData: objects) {

            String guid = objData.getGuid();
            String wsid = new GUID(guid).getAccessGroupId().toString();
            if (wsNarrativeInfo == null) {
                wsNarrativeInfo = new HashMap<>();
            }
            if (!wsNarrativeInfo.containsKey(wsid)) {
                // get workspace info meta data
                WorkspaceClient wsClient = getWorkspaceClient();
                WorkspaceEventHandler weh = new WorkspaceEventHandler(
                                                new CloneableWorkspaceClientImpl(wsClient));
                Map<String, String> wsMetaInfo = weh.getWorkspaceInfo(Integer.parseInt(wsid));

                if ( wsMetaInfo.containsKey("narrative") &&
                        wsMetaInfo.containsKey("narrative_nice_name") ) {
                    narrativeID = Long.parseLong(wsMetaInfo.get("narrative"));
                    narrativeName = wsMetaInfo.get("narrative_nice_name");

                    NarrativeInfo tempNarrInfo = new NarrativeInfo().
                            withNarrativeId(narrativeID).
                            withNarrativeName(narrativeName);
                    wsNarrativeInfo.put(wsid, tempNarrInfo);
                }
            }
        }
        return wsNarrativeInfo;
    }
}
