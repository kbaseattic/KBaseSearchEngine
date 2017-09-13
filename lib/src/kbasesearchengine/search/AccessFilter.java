package kbasesearchengine.search;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class AccessFilter {
    public boolean isAdmin = false;
    public boolean withPublic = false;
    public Set<Integer> accessGroupIds = null;
    public boolean withAllHistory = false;
    
    public AccessFilter() {}
    
    public static AccessFilter create() {
        return new AccessFilter();
    }
    
    public AccessFilter withAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
        return this;
    }
    
    public AccessFilter withPublic(boolean withPublic) {
        this.withPublic = withPublic;
        return this;
    }
    
    public AccessFilter withAccessGroups(Set<Integer> accessGroupIds) {
        this.accessGroupIds = accessGroupIds;
        return this;
    }

    public AccessFilter withAccessGroups(Integer... accessGroupIds) {
        this.accessGroupIds = new LinkedHashSet<Integer>(Arrays.asList(accessGroupIds));
        return this;
    }
    
    public AccessFilter withAllHistory(boolean withAllHistory) {
        this.withAllHistory = withAllHistory;
        return this;
    }
}
