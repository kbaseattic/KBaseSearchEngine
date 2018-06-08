package kbasesearchengine.main;

import java.io.IOException;
import kbasesearchengine.main.NarrativeInfo;
import us.kbase.common.service.JsonClientException;

public interface NarrativeInfoProvider {
    public NarrativeInfo findNarrativeInfo(Long wsId)
            throws JsonClientException, IOException;
}
