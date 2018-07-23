package kbasesearchengine.main;

import java.util.Map;

import us.kbase.common.service.Tuple9;

public interface AccessGroupInfoProvider {
    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> getAccessGroupInfo(Long accessGroupId);
}
