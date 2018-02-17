package kbasesearchengine.main;

import us.kbase.common.service.Tuple5;
import java.io.IOException;

public interface NarrativeInfoProvider {
    public Tuple5<String, Long, Long, String, String> findNarrativeInfo(Long wsId) throws IOException;
}
