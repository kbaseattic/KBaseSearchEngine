package kbaserelationengine.parse;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.Tuple11;
import workspace.GetObjects2Params;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class ObjectParser {
    public void filterSubObjects(URL wsUrl, File tempDir, AuthToken token, 
            String objRef, String pathToSub, String relPathToId) throws Exception {
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        File tempFile = File.createTempFile("ws_srv_response_", ".json", tempDir);
        WorkspaceClient wc = new WorkspaceClient(wsUrl, token);
        wc.setIsInsecureHttpConnectionAllowed(true);
        wc.setStreamingModeOn(true);
        wc._setFileForNextRpcResponse(tempFile);
        ObjectData obj = wc.getObjects2(new GetObjects2Params().withObjects(
                Arrays.asList(new ObjectSpecification().withRef(objRef)))).getData().get(0);
        String resolvedRef = getRefFromObjectInfo(obj.getInfo());
        obj.getData().getPlacedStream();
    }
    
    public static String getRefFromObjectInfo(Tuple11<Long, String, String, String, 
            Long, String, Long, String, String, Long, Map<String,String>> info) {
        return info.getE7() + "/" + info.getE1() + "/" + info.getE5();
    }
}
