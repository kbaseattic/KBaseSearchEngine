package kbaserelationengine.search;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.common.GUID;

public class AccessInfo {
    public GUID pguid;
    public GUID prefix;
    public Integer version;
    public Set<Integer> lastIn;
    public Set<Integer> groups;
    
    @Override
    public String toString() {
        return "AccessInfo [pguid=" + pguid + ", prefix=" + prefix + ", version="
                + version + ", lastIn=" + lastIn + ", groups=" + groups + "]";
    }

    @SuppressWarnings("unchecked")
    public static AccessInfo fromMap(Map<String, Object> data) {
        AccessInfo ret = new AccessInfo();
        ret.pguid = new GUID((String)data.get("pguid"));
        ret.prefix = new GUID((String)data.get("prefix"));
        ret.version = (Integer)data.get("version");
        ret.lastIn = new LinkedHashSet<Integer>((List<Integer>)data.get("lastin"));
        ret.groups = new LinkedHashSet<Integer>((List<Integer>)data.get("groups"));
        return ret;
    }
    
    @SuppressWarnings("serial")
    public Map<String, Object> toMap() {
        return new LinkedHashMap<String, Object>() {{
            put("pguid", pguid.toString());
            put("prefix", prefix.toString());
            put("version", version);
            put("lastin", lastIn);
            put("groups", groups);
        }};
    }
}
