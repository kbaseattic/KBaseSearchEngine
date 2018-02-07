package kbasesearchengine.search;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class AccessFilter {
    
    //TODO CODE everything about this class
    
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((accessGroupIds == null) ? 0 : accessGroupIds.hashCode());
        result = prime * result + (isAdmin ? 1231 : 1237);
        result = prime * result + (withAllHistory ? 1231 : 1237);
        result = prime * result + (withPublic ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AccessFilter other = (AccessFilter) obj;
        if (accessGroupIds == null) {
            if (other.accessGroupIds != null) {
                return false;
            }
        } else if (!accessGroupIds.equals(other.accessGroupIds)) {
            return false;
        }
        if (isAdmin != other.isAdmin) {
            return false;
        }
        if (withAllHistory != other.withAllHistory) {
            return false;
        }
        if (withPublic != other.withPublic) {
            return false;
        }
        return true;
    }

}
