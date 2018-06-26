package kbasesearchengine.search;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/**
 * Describes what objects in {@link IndexingStorage} a search should return.
 * 
 * @author gaprice@lbl.gov
 * @author rsutormin@lbl.gov
 *
 */
public class MatchFilter {

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

    /** True if sub objects should be excluded from the search, default false.
     * @return true if sub objects should be excluded.
     */
    public boolean isExcludeSubObjects() {
        return excludeSubObjects;
    }

    /** A text string to match across all fields of an object, if present.
     * @return the text string.
     */
    public Optional<String> getFullTextInAll() {
        return fullTextInAll;
    }

    /** An object name to match across objects, if present.
     * @return the object name.
     */
    public Optional<String> getObjectName() {
        return objectName;
    }

    /** A time range to match across objects, if present.
     * @return the time range.
     */
    public Optional<MatchValue> getTimestamp() {
        return timestamp;
    }

    /** A set of matches on object keys to search on.
     * @return the matches.
     */
    public Map<String, MatchValue> getLookupInKeys() {
        return lookupInKeys;
    }

    /** Tags applied to objects at the data source to apply to the search.
     * @return data source tags.
     */
    public Set<String> getSourceTags() {
        return sourceTags;
    }

    /** Returns true if the data source tags is a blacklist, or false if a whitelist
     * (the default).
     * @return whether the data source tags is a whitelist or blacklist.
     */
    public boolean isSourceTagsBlacklist() {
        return isSourceTagsBlacklist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchFilter that = (MatchFilter) o;

        if (excludeSubObjects != that.excludeSubObjects) return false;
        if (isSourceTagsBlacklist != that.isSourceTagsBlacklist) return false;
        if (fullTextInAll != null ? !fullTextInAll.equals(that.fullTextInAll) : that.fullTextInAll != null)
            return false;
        if (objectName != null ? !objectName.equals(that.objectName) : that.objectName != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (lookupInKeys != null ? !lookupInKeys.equals(that.lookupInKeys) : that.lookupInKeys != null) return false;
        return sourceTags != null ? sourceTags.equals(that.sourceTags) : that.sourceTags == null;
    }

    @Override
    public int hashCode() {
        int result = (excludeSubObjects ? 1 : 0);
        result = 31 * result + (fullTextInAll != null ? fullTextInAll.hashCode() : 0);
        result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (lookupInKeys != null ? lookupInKeys.hashCode() : 0);
        result = 31 * result + (sourceTags != null ? sourceTags.hashCode() : 0);
        result = 31 * result + (isSourceTagsBlacklist ? 1 : 0);
        return result;
    }

    /** Get a builder for a {@link MatchFilter}.
     * @return
     */
    public static Builder getBuilder() {
        return new Builder();
    }
    
    /** A {@link MatchFilter} builder.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private boolean excludeSubObjects = false;
        private String fullTextInAll = null;
        private String objectName = null;
        private MatchValue timestamp = null;
        private Map<String, MatchValue> lookupInKeys = new HashMap<>();
        private Set<String> sourceTags = new HashSet<>();
        private boolean isSourceTagsBlacklist = false;

        private Builder() {}

        /** Set whether subobjects should be returned in the search results, default false.
         * @param excludeSubObjects whether subobjects should be returned.
         * @return this builder.
         */
        public Builder withExcludeSubObjects(final boolean excludeSubObjects) {
            this.excludeSubObjects = excludeSubObjects;
            return this;
        }
        
        /** Add a text string that is compared across all object fields for matches when searching.
         * @param text the text string.
         * @return this builder.
         */
        public Builder withNullableFullTextInAll(final String text) {
            this.fullTextInAll = text;
            return this;
        }
        
        /** Add an object name to search against.
         * @param objectName the object name.
         * @return this builder.
         */
        public Builder withNullableObjectName(final String objectName) {
            this.objectName = objectName;
            return this;
        }
        
        /** Add a range that constrains the creation date of the returned objects.
         * @param value the timestamp range.
         * @return this builder.
         */
        public Builder withNullableTimestamp(final MatchValue value) {
            this.timestamp = value;
            return this;
        }
        
        /** Add a key / value pair to search on.
         * @param keyName the object key to match.
         * @param value the value of the key to match.
         * @return this builder.
         */
        public Builder withLookupInKey(final String keyName, final String value) {
            Utils.notNullOrEmpty(value, "value cannot be null or whitespace only");
            return withLookupInKey(keyName, new MatchValue(value));
        }
        
        /** Add a key / value pair to search on.
         * @param keyName the object key to match.
         * @param value the value of the key to match.
         * @return this builder.
         */
        public Builder withLookupInKey(final String keyName, final MatchValue value) {
            Utils.notNullOrEmpty(keyName, "key cannot be null or whitespace only");
            Utils.nonNull(value, "value");
            this.lookupInKeys.put(keyName, value);
            return this;
        }
        
        /** Add a tag applied at the data source to match against in the search.
         * @param sourceTag the data source tag.
         * @return this builder.
         */
        public Builder withSourceTag(final String sourceTag) {
            Utils.notNullOrEmpty(sourceTag, "sourceTag cannot be null or whitespace only");
            sourceTags.add(sourceTag);
            return this;
        }
        
        /** Specify whether the data source tags should be applied as a blacklist or a whitelist,
         * the default.
         * @param isBlacklist true if the data source tags should be treated as a blacklist.
         * @return this builder.
         */
        public Builder withIsSourceTagsBlackList(final boolean isBlacklist) {
            isSourceTagsBlacklist = isBlacklist;
            return this;
        }

        /** Build the {@link MatchFilter}.
         * @return this builder. the new {@link MatchFilter}.
         */
        public MatchFilter build() {
            return new MatchFilter(excludeSubObjects, fullTextInAll, objectName, timestamp, lookupInKeys,
                    sourceTags, isSourceTagsBlacklist);
        }
    }

}
