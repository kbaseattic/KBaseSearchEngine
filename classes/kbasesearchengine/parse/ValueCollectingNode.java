package kbasesearchengine.parse;

import java.util.LinkedHashMap;
import java.util.Map;

import kbasesearchengine.common.ObjectJsonPath;

public class ValueCollectingNode <T> {
    private T rules = null;
    private Map<String, ValueCollectingNode<T>> children = null;
    
    public ValueCollectingNode() {
    }
    
    public Map<String, ValueCollectingNode<T>> getChildren() {
        return children;
    }
    
    public void addChild(String key, ValueCollectingNode<T> child) {
        if (children == null) 
            children = new LinkedHashMap<String, ValueCollectingNode<T>>();
        children.put(key, child);
    }

    public boolean hasChildren() {
        return children != null && children.size() > 0;
    }
    
    public T getRules() {
        return rules;
    }
    
    public void setRules(T rules) {
        this.rules = rules;
    }

    public ValueCollectingNode<T> addPath(ObjectJsonPath jsonPath, T rules) {
        String[] path = jsonPath.getPathItems();
        if (path.length == 0 || path[0].isEmpty()) {
            this.setRules(rules);
            return this;
        } else {
            return addPath(path, 0, rules);
        }
    }
    
    private ValueCollectingNode<T> addPath(String[] path, int pos, T rules) {
        if (pos >= path.length) {
            this.setRules(rules);
            return this;
        } else {
            String key = path[pos];
            ValueCollectingNode<T> child = null;
            if (getChildren() == null || !getChildren().containsKey(key)) {
                child = new ValueCollectingNode<T>();
                addChild(key, child);
            } else {
                child = getChildren().get(key);
            }
            return child.addPath(path, pos + 1, rules);
        }
    }
}
