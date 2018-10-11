package kbasesearchengine.main;

import kbasesearchengine.main.NarrativeInfo;
import us.kbase.common.service.JsonClientException;

import java.io.IOException;

public interface NarrativeInfoProvider {
    public NarrativeInfo findNarrativeInfo(Long wsId) throws IOException, JsonClientException;
}
