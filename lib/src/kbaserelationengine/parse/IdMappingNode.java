package kbaserelationengine.parse;

import java.util.LinkedHashMap;
import java.util.Map;

import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.system.RelationRules;

public class IdMappingNode {
    private boolean primary = false;
    private RelationRules foreignKeyLookupRules = null;

    private Map<String, IdMappingNode> children = null;
    
    public IdMappingNode() {
    }
    
    public Map<String, IdMappingNode> getChildren() {
        return children;
    }
    
    public void addChild(String key, IdMappingNode child) {
        if (children == null) 
            children = new LinkedHashMap<String, IdMappingNode>();
        children.put(key, child);
    }

    public boolean hasChildren() {
        return children != null && children.size() > 0;
    }
    
    public boolean isPrimary() {
        return primary;
    }
    
    public void setPrimary(boolean primary) {
        this.primary = primary;
    }
    
    public RelationRules getForeignKeyLookupRules() {
        return foreignKeyLookupRules;
    }
    
    public void setForeignKeyLookupRules(RelationRules foreignKeyLookupRules) {
        this.foreignKeyLookupRules = foreignKeyLookupRules;
    }

    public IdMappingNode addPath(ObjectJsonPath jsonPath, boolean primary,
            RelationRules foreignKeyLookupRules) {
        String[] path = jsonPath.getPathItems();
        if (path.length == 0 || path[0].isEmpty()) {
            this.primary = primary;
            this.foreignKeyLookupRules = foreignKeyLookupRules;
            return this;
        } else {
            return addPath(path, 0, primary, foreignKeyLookupRules);
        }
    }
    
    private IdMappingNode addPath(String[] path, int pos, boolean primary,
            RelationRules foreignKeyLookupRules) {
        if (pos >= path.length) {
            this.primary = primary;
            this.foreignKeyLookupRules = foreignKeyLookupRules;
            return this;
        } else {
            String key = path[pos];
            IdMappingNode child = null;
            if (getChildren() == null || !getChildren().containsKey(key)) {
                child = new IdMappingNode();
                addChild(key, child);
            } else {
                child = getChildren().get(key);
            }
            return child.addPath(path, pos + 1, primary, foreignKeyLookupRules);
        }
    }
}
