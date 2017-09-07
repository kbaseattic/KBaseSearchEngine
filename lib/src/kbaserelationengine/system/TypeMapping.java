package kbaserelationengine.system;

import com.google.common.base.Optional;

import kbaserelationengine.tools.Utils;

/** A mapping of a type from a storage type to a search type.
 * 
 * Mappings may be standard or default mappings. A default mapping is a mapping to use if there
 * is no mapping for a storage type with a specific version. In that case, the storage type can
 * be mapped using the versionless default mapping. 
 * @author gaprice@lbl.gov
 *
 */
public class TypeMapping {
    
    //TODO TEST
    
    private StorageObjectType storageType;
    private String searchType;
    private Optional<String> sourceInfo;
    private boolean isDefault;
    
    /** Create a type mapping.
     * @param storageType the storage type.
     * @param searchType the search type to which the storage type maps.
     * @param sourceInfo information about the source of a mapping - this is often a file name and
     * information about the mapping's location in the file.
     * @param isDefault whether this is a default mapping or not. If true, a version may not be
     * specified in the storage type.
     */
    public TypeMapping(
            final StorageObjectType storageType,
            final String searchType,
            final String sourceInfo,
            final boolean isDefault) {
        Utils.nonNull(storageType, "storageType");
        Utils.notNullOrEmpty(searchType, "searchType is null or empty");
        if (sourceInfo != null && sourceInfo.isEmpty()) {
            this.sourceInfo = Optional.absent();
        } else {
            this.sourceInfo = Optional.of(sourceInfo);
        }
        this.isDefault = isDefault;
        this.storageType = storageType;
        this.searchType = searchType;
        if (isDefault && storageType.getVersion().isPresent()) {
            throw new IllegalArgumentException(
                    "Default mappings may not have a version specified in the storage type");
        }
    }

    /** Get the storage type.
     * @return the storage type.
     */
    public StorageObjectType getStorageType() {
        return storageType;
    }

    /** Get the search type to which the storage type maps.
     * @return the search type.
     */
    public String getSearchType() {
        return searchType;
    }

    /** Get information about the source of the mapping, if any.
     * @return the source information for the mapping, or absent if missing.
     */
    public Optional<String> getSourceInfo() {
        return sourceInfo;
    }

    /** Whether this mapping is a default mapping or not.
     * @return true if the mapping is a default mapping.
     */
    public boolean isDefault() {
        return isDefault;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TypeMapping [storageType=");
        builder.append(storageType);
        builder.append(", searchType=");
        builder.append(searchType);
        builder.append(", sourceInfo=");
        builder.append(sourceInfo);
        builder.append(", isDefault=");
        builder.append(isDefault);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isDefault ? 1231 : 1237);
        result = prime * result
                + ((searchType == null) ? 0 : searchType.hashCode());
        result = prime * result
                + ((sourceInfo == null) ? 0 : sourceInfo.hashCode());
        result = prime * result
                + ((storageType == null) ? 0 : storageType.hashCode());
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
        if (isDefault != other.isDefault) {
            return false;
        }
        if (searchType == null) {
            if (other.searchType != null) {
                return false;
            }
        } else if (!searchType.equals(other.searchType)) {
            return false;
        }
        if (sourceInfo == null) {
            if (other.sourceInfo != null) {
                return false;
            }
        } else if (!sourceInfo.equals(other.sourceInfo)) {
            return false;
        }
        if (storageType == null) {
            if (other.storageType != null) {
                return false;
            }
        } else if (!storageType.equals(other.storageType)) {
            return false;
        }
        return true;
    }

    
}
