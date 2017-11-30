package kbasesearchengine.parse;

import java.util.LinkedHashMap;
import java.util.Map;

import kbasesearchengine.common.ObjectJsonPath;

/**
 * This class describes schema of subdata extraction.
 * @author rsutormin
 */
public class SubObjectExtractionNode {
	private boolean needAll = false;
	private boolean skipLevel = false;
	private Map<String, SubObjectExtractionNode> children = null;
	
	public SubObjectExtractionNode() {
	}
	
	public Map<String, SubObjectExtractionNode> getChildren() {
		return children;
	}
	
	public void addChild(String key, SubObjectExtractionNode child) {
		if (children == null) 
			children = new LinkedHashMap<String, SubObjectExtractionNode>();
		children.put(key, child);
	}

	public boolean hasChildren() {
		return children != null && children.size() > 0;
	}

	public boolean isNeedAll() {
		return needAll;
	}
	
	public void setNeedAll(boolean needAll) {
		this.needAll = needAll;
	}
	
	public boolean isSkipLevel() {
        return skipLevel;
    }
	
	public void setSkipLevel(boolean skipLevel) {
        this.skipLevel = skipLevel;
    }
	
	public SubObjectExtractionNode addPath(ObjectJsonPath jsonPath, boolean skipLevels,
	        boolean markLeavesNeedAll) {
	    String[] path = jsonPath.getPathItems();
		if (path.length == 0 || path[0].isEmpty()) {
		    skipLevel = skipLevels;
			needAll = markLeavesNeedAll;
			return this;
		} else {
			return addPath(path, 0, skipLevels, markLeavesNeedAll);
		}
	}
	
	private SubObjectExtractionNode addPath(String[] path, int pos, boolean skipLevels,
	        boolean markLeavesNeedAll) {
	    skipLevel = skipLevels;
		if (pos >= path.length) {
			needAll = markLeavesNeedAll;
			return this;
		} else {
			String key = path[pos];
			SubObjectExtractionNode child = null;
			if (getChildren() == null || !getChildren().containsKey(key)) {
				child = new SubObjectExtractionNode();
				addChild(key, child);
			} else {
				child = getChildren().get(key);
			}
			return child.addPath(path, pos + 1, skipLevels, markLeavesNeedAll);
		}
	}
}
