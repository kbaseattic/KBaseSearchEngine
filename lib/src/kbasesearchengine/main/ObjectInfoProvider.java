package kbasesearchengine.main;

import java.io.IOException;
import java.util.Map;

import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;

public interface ObjectInfoProvider {
    public Map<String, Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>>> getObjectsInfo(Iterable <? extends String> objRefs)
            throws JsonClientException, IOException;
    public Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>> getObjectInfo(String objRef)
            throws JsonClientException, IOException;
}
