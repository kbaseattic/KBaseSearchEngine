package kbasesearchengine.search;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.common.GUID;
import kbasesearchengine.tools.Utils;

public class MatchFilter {
    
    //TODO CODE everything about this class
    
    public boolean excludeSubObjects = false;
    public String fullTextInAll = null;
    public Integer accessGroupId = null;
    public String objectName = null;
    public GUID parentGuid = null;
    public MatchValue timestamp = null;
    public Map<String, MatchValue> lookupInKeys = null;
    public Set<String> sourceTags = new HashSet<>();
    public boolean isSourceTagsBlacklist = false;

    public MatchFilter() {}
    
    public static MatchFilter create() {
        return new MatchFilter();
    }
    
    public MatchFilter withExcludeSubObjects(final boolean excludeSubObjects) {
        this.excludeSubObjects = excludeSubObjects;
        return this;
    }
    
    public MatchFilter withFullTextInAll(String text) {
        this.fullTextInAll = text;
        return this;
    }
    
    public MatchFilter withAccessGroupId(Integer accessGroupId) {
        this.accessGroupId = accessGroupId;
        return this;
    }
    
    public MatchFilter withObjectName(String partOfName) {
        this.objectName = partOfName;
        return this;
    }
    
    public MatchFilter withParentGuid(GUID parentGuid) {
        this.parentGuid = parentGuid;
        return this;
    }
    
    public MatchFilter withTimestamp(MatchValue value) {
        this.timestamp = value;
        return this;
    }
    
    public MatchFilter withLookupInKeys(Map<String, MatchValue> keys) {
        if (lookupInKeys == null) {
            this.lookupInKeys = new LinkedHashMap<>(keys);
        } else {
            this.lookupInKeys.putAll(keys);
        }
        return this;
    }

    public MatchFilter withLookupInKey(String keyName, String value) {
        return withLookupInKey(keyName, new MatchValue(value));
    }

    public MatchFilter withLookupInKey(String keyName, MatchValue value) {
        if (this.lookupInKeys == null) {
            this.lookupInKeys = new LinkedHashMap<>();
        }
        this.lookupInKeys.put(keyName, value);
        return this;
    }
    
    public MatchFilter withSourceTag(final String sourceTag) {
        Utils.notNullOrEmpty(sourceTag, "sourceTag cannot be null or whitespace only");
        sourceTags.add(sourceTag);
        return this;
    }
    
    public MatchFilter withIsSourceTagsBlackList(final Boolean isBlacklist) {
        isSourceTagsBlacklist = isBlacklist != null && isBlacklist;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((accessGroupId == null) ? 0 : accessGroupId.hashCode());
        result = prime * result + (excludeSubObjects ? 1231 : 1237);
        result = prime * result
                + ((fullTextInAll == null) ? 0 : fullTextInAll.hashCode());
        result = prime * result + (isSourceTagsBlacklist ? 1231 : 1237);
        result = prime * result
                + ((lookupInKeys == null) ? 0 : lookupInKeys.hashCode());
        result = prime * result
                + ((objectName == null) ? 0 : objectName.hashCode());
        result = prime * result
                + ((parentGuid == null) ? 0 : parentGuid.hashCode());
        result = prime * result
                + ((sourceTags == null) ? 0 : sourceTags.hashCode());
        result = prime * result
                + ((timestamp == null) ? 0 : timestamp.hashCode());
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
        MatchFilter other = (MatchFilter) obj;
        if (accessGroupId == null) {
            if (other.accessGroupId != null) {
                return false;
            }
        } else if (!accessGroupId.equals(other.accessGroupId)) {
            return false;
        }
        if (excludeSubObjects != other.excludeSubObjects) {
            return false;
        }
        if (fullTextInAll == null) {
            if (other.fullTextInAll != null) {
                return false;
            }
        } else if (!fullTextInAll.equals(other.fullTextInAll)) {
            return false;
        }
        if (isSourceTagsBlacklist != other.isSourceTagsBlacklist) {
            return false;
        }
        if (lookupInKeys == null) {
            if (other.lookupInKeys != null) {
                return false;
            }
        } else if (!lookupInKeys.equals(other.lookupInKeys)) {
            return false;
        }
        if (objectName == null) {
            if (other.objectName != null) {
                return false;
            }
        } else if (!objectName.equals(other.objectName)) {
            return false;
        }
        if (parentGuid == null) {
            if (other.parentGuid != null) {
                return false;
            }
        } else if (!parentGuid.equals(other.parentGuid)) {
            return false;
        }
        if (sourceTags == null) {
            if (other.sourceTags != null) {
                return false;
            }
        } else if (!sourceTags.equals(other.sourceTags)) {
            return false;
        }
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        return true;
    }
}
