package kbasesearchengine.search;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

public class MatchFilter {
    
    //TODO NNOW JAVADOC
    
    private final boolean excludeSubObjects;
    private final Optional<String> fullTextInAll;
    private final Optional<String> objectName;
    private final Optional<MatchValue> timestamp;
    private final Map<String, MatchValue> lookupInKeys;
    private final Set<String> sourceTags;
    private final boolean isSourceTagsBlacklist;

    private MatchFilter(
            final boolean excludeSubObjects,
            final String fullTextInAll,
            final String objectName,
            final MatchValue timestamp, //TODO CODE this is gross. Make an actual date range class
            final Map<String, MatchValue> lookupInKeys,
            final Set<String> sourceTags,
            final boolean isSourceTagsBlacklist) {
        this.excludeSubObjects = excludeSubObjects;
        this.fullTextInAll = Optional.fromNullable(fullTextInAll);
        this.objectName = Optional.fromNullable(objectName);
        this.timestamp = Optional.fromNullable(timestamp);
        this.lookupInKeys = Collections.unmodifiableMap(lookupInKeys);
        this.sourceTags = Collections.unmodifiableSet(sourceTags);
        this.isSourceTagsBlacklist = isSourceTagsBlacklist;
    }

    public boolean isExcludeSubObjects() {
        return excludeSubObjects;
    }

    public Optional<String> getFullTextInAll() {
        return fullTextInAll;
    }

    public Optional<String> getObjectName() {
        return objectName;
    }

    public Optional<MatchValue> getTimestamp() {
        return timestamp;
    }

    public Map<String, MatchValue> getLookupInKeys() {
        return lookupInKeys;
    }

    public Set<String> getSourceTags() {
        return sourceTags;
    }

    public boolean isSourceTagsBlacklist() {
        return isSourceTagsBlacklist;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (excludeSubObjects ? 1231 : 1237);
        result = prime * result
                + ((fullTextInAll == null) ? 0 : fullTextInAll.hashCode());
        result = prime * result + (isSourceTagsBlacklist ? 1231 : 1237);
        result = prime * result
                + ((lookupInKeys == null) ? 0 : lookupInKeys.hashCode());
        result = prime * result
                + ((objectName == null) ? 0 : objectName.hashCode());
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

    public static Builder getBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        
        private boolean excludeSubObjects = false;
        private String fullTextInAll = null;
        private String objectName = null;
        private MatchValue timestamp = null;
        private Map<String, MatchValue> lookupInKeys = new HashMap<>();
        private Set<String> sourceTags = new HashSet<>();
        private boolean isSourceTagsBlacklist = false;
        
        private Builder() {}

        public Builder withExcludeSubObjects(final boolean excludeSubObjects) {
            this.excludeSubObjects = excludeSubObjects;
            return this;
        }
        
        public Builder withNullableFullTextInAll(final String text) {
            this.fullTextInAll = text;
            return this;
        }
        
        public Builder withNullableObjectName(final String objectName) {
            this.objectName = objectName;
            return this;
        }
        
        public Builder withNullableTimestamp(final MatchValue value) {
            this.timestamp = value;
            return this;
        }
        
        public Builder withLookupInKey(final String keyName, final String value) {
            Utils.notNullOrEmpty(value, "value cannot be null or whitespace only");
            return withLookupInKey(keyName, new MatchValue(value));
        }
        
        public Builder withLookupInKey(final String keyName, final MatchValue value) {
            Utils.notNullOrEmpty(keyName, "key cannot be null or whitespace only");
            Utils.nonNull(value, "value");
            this.lookupInKeys.put(keyName, value);
            return this;
        }
        
        public Builder withSourceTag(final String sourceTag) {
            Utils.notNullOrEmpty(sourceTag, "sourceTag cannot be null or whitespace only");
            sourceTags.add(sourceTag);
            return this;
        }
        
        public Builder withIsSourceTagsBlackList(final boolean isBlacklist) {
            isSourceTagsBlacklist = isBlacklist;
            return this;
        }
        
        public MatchFilter build() {
            return new MatchFilter(excludeSubObjects, fullTextInAll, objectName, timestamp,
                    lookupInKeys, sourceTags, isSourceTagsBlacklist);
        }
    }

}
