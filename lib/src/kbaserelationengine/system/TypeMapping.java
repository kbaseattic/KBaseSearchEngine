package kbaserelationengine.system;

import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Optional;

import kbaserelationengine.tools.Utils;

/** A mapping of a type from a storage type to a search type.
 * 
 * Mappings come in two flavors - version specific mappings from a storage type and version to a
 * search type, and default mappings that map from a storage type without a version to a search
 * type.
 * @author gaprice@lbl.gov
 *
 */
public class TypeMapping {
    
    //TODO TEST
    
    private final String storageCode;
    private final String storageType;
    private final Optional<String> sourceInfo;
    private final Optional<String> defaultSearchType;
    private final Map<Integer, String> versions;
    
    private TypeMapping(
            final String storageCode,
            final String storageType,
            final Optional<String> sourceInfo,
            final Optional<String> defaultSearchType,
            final Map<Integer, String> versions) {
        this.storageCode = storageCode;
        this.storageType = storageType;
        this.sourceInfo = sourceInfo;
        this.defaultSearchType = defaultSearchType;
        this.versions = versions;
    }

    /** Return the storage code for this type mapping.
     * @return the source data storage code.
     */
    public String getStorageCode() {
        return storageCode;
    }

    /** Get the storage type for this mapping.
     * @return the source data storage type, without a version.
     */
    public String getStorageType() {
        return storageType;
    }

    /** Get information about the source of the mapping, if any.
     * @return the source information for the mapping, or absent if missing.
     */
    public Optional<String> getSourceInfo() {
        return sourceInfo;
    }
    
    /** Get the search type for a particular version. If no version is available, pass null in
     * its place.
     * @param version the version number, or null if there is no version.
     * @return the mapped search type, or absent if no search type mapping is available.
     */
    public Optional<String> getSearchType(final Integer version) {
        if (version != null & version < 0) {
            throw new IllegalArgumentException("version must be at least 0");
        }
        if (!versions.containsKey(version)) {
            if (defaultSearchType.isPresent()) {
                return defaultSearchType;
            }
        } else {
            return Optional.of(versions.get(version));
        }
        return Optional.absent();
    }

    @Override
    public String toString() {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("TypeMapping [storageCode=");
        builder2.append(storageCode);
        builder2.append(", storageType=");
        builder2.append(storageType);
        builder2.append(", sourceInfo=");
        builder2.append(sourceInfo);
        builder2.append(", defaulSearchType=");
        builder2.append(defaultSearchType);
        builder2.append(", versions=");
        builder2.append(versions);
        builder2.append("]");
        return builder2.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((defaultSearchType == null) ? 0
                : defaultSearchType.hashCode());
        result = prime * result
                + ((sourceInfo == null) ? 0 : sourceInfo.hashCode());
        result = prime * result
                + ((storageCode == null) ? 0 : storageCode.hashCode());
        result = prime * result
                + ((storageType == null) ? 0 : storageType.hashCode());
        result = prime * result
                + ((versions == null) ? 0 : versions.hashCode());
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
        TypeMapping other = (TypeMapping) obj;
        if (defaultSearchType == null) {
            if (other.defaultSearchType != null) {
                return false;
            }
        } else if (!defaultSearchType.equals(other.defaultSearchType)) {
            return false;
        }
        if (sourceInfo == null) {
            if (other.sourceInfo != null) {
                return false;
            }
        } else if (!sourceInfo.equals(other.sourceInfo)) {
            return false;
        }
        if (storageCode == null) {
            if (other.storageCode != null) {
                return false;
            }
        } else if (!storageCode.equals(other.storageCode)) {
            return false;
        }
        if (storageType == null) {
            if (other.storageType != null) {
                return false;
            }
        } else if (!storageType.equals(other.storageType)) {
            return false;
        }
        if (versions == null) {
            if (other.versions != null) {
                return false;
            }
        } else if (!versions.equals(other.versions)) {
            return false;
        }
        return true;
    }

    /** Get a builder for a type mapping.
     * @param storageCode the storage code for the source data storage.
     * @param storageType the type of the source data type.
     * @return
     */
    public static Builder getBuilder(final String storageCode, final String storageType) {
        return new Builder(storageCode, storageType);
    }
    
    /** A type mapping builder.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {

        private final String storageCode;
        private final String storageType;
        private Optional<String> sourceInfo = Optional.absent();
        private Optional<String> defaultSearchType = Optional.absent();
        private final Map<Integer, String> versions = new TreeMap<>();
        
        private Builder(final String storageCode, final String storageType) {
            Utils.notNullOrEmpty(storageCode, "storageCode");
            Utils.notNullOrEmpty(storageType, "storageType");
            this.storageCode = storageCode;
            this.storageType = storageType;
        }
        
        /** Specify information about the source of this type mapping. This is often a filename.
         * Null or whitespace only entries will be ignored.
         * @param sourceInfo the source information.
         * @return this builder.
         */
        public Builder withNullableSourceInfo(final String sourceInfo) {
            if (Utils.isNullOrEmpty(sourceInfo)) {
                this.sourceInfo = Optional.absent();
            } else {
                this.sourceInfo = Optional.of(sourceInfo);
            }
            return this;
        }
        
        /** Specify a default search type to map to the storage type when a version specific
         * mapping is not available. Null or whitespace only entries will be ignored.
         * @param searchType the default search type.
         * @return this builder.
         */
        public Builder withNullableDefaultSearchType(final String searchType) {
            if (Utils.isNullOrEmpty(searchType)) {
                defaultSearchType = Optional.absent();
            } else {
                defaultSearchType = Optional.of(searchType);
            }
            return this;
        }
        
        /** Specify a mapping for a specific version of the storage type to a search type.
         * @param version the version of the storage type.
         * @param searchType the search type to which the version will be mapped.
         * @return this builder.
         */
        public Builder withVersion(final int version, final String searchType) {
            if (version < 0) {
                throw new IllegalArgumentException("version must be at least 0");
            }
            Utils.notNullOrEmpty(searchType, "searchType");
            versions.put(version, searchType);
            return this;
        }
        
        /** Check if the builder is in a build-ready state. This means that either the default
         * search type or at least one version mapping as been added to the build. 
         * @return true if the builder is ready to build.
         */
        public boolean buildReady() {
            return defaultSearchType.isPresent() || !versions.isEmpty();
        }
        
        /** Build the type mapping.
         * @return the type mapping.
         */
        public TypeMapping build() {
            if (!defaultSearchType.isPresent() && versions.isEmpty()) {
                throw new IllegalStateException("No type mappings were supplied");
            }
            return new TypeMapping(
                    storageCode, storageType, sourceInfo, defaultSearchType, versions);
        }
        
    }
}
