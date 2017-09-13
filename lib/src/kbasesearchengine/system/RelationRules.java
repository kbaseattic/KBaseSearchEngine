package kbasesearchengine.system;

import kbasesearchengine.common.ObjectJsonPath;

public class RelationRules {
    private ObjectJsonPath path;
    private String targetObjectType;
    private String relationType;
    
    public ObjectJsonPath getPath() {
        return path;
    }
    
    public void setPath(ObjectJsonPath path) {
        this.path = path;
    }
    
    public String getTargetObjectType() {
        return targetObjectType;
    }
    
    public void setTargetObjectType(String targetObjectType) {
        this.targetObjectType = targetObjectType;
    }
    
    public String getRelationType() {
        return relationType;
    }
    
    public void setRelationType(String relationType) {
        this.relationType = relationType;
    }

    @Override
    public String toString() {
        return "RelationRules [path=" + path + ", targetObjectType="
                + targetObjectType + ", relationType=" + relationType + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result
                + ((relationType == null) ? 0 : relationType.hashCode());
        result = prime * result + ((targetObjectType == null) ? 0
                : targetObjectType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RelationRules other = (RelationRules) obj;
        if (path == null) {
            if (other.path != null)
                return false;
        } else if (!path.equals(other.path))
            return false;
        if (relationType == null) {
            if (other.relationType != null)
                return false;
        } else if (!relationType.equals(other.relationType))
            return false;
        if (targetObjectType == null) {
            if (other.targetObjectType != null)
                return false;
        } else if (!targetObjectType.equals(other.targetObjectType))
            return false;
        return true;
    }

}
