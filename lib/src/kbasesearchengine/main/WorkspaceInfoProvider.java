package kbasesearchengine.main;

import java.io.IOException;
import java.util.Map;

import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple9;

public interface WorkspaceInfoProvider {
    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> getWorkspaceInfo(Long wsId)
            throws JsonClientException, IOException;
}
