package kbasesearchengine.system;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/** The type of an object in a data storage system. Consists of a storage code, an arbitrary name
 * and an optional version.
 * @author gaprice@lbl.gov
 *
 */
public class StorageObjectType {
    
    private final String storageCode;
    private final String type;
    private final Optional<Integer> version;
    
    /** Create an versionless type.
     * @param storageCode the storage code.
     * @param type the type name.
     */
    public StorageObjectType(final String storageCode, final String type) {
        Utils.notNullOrEmpty(storageCode, "storageCode cannot be null or the empty string");
        this.storageCode = storageCode;
        Utils.notNullOrEmpty(type, "type cannot be null or the empty string");
        this.type = type;
        this.version = Optional.absent();
    }
    
    /** Create a type with a version.
     * @param storageCode the storage code.
     * @param type the type name.
     * @param version the type version.
     */
    public StorageObjectType(final String storageCode, final String type, final int version) {
        Utils.notNullOrEmpty(storageCode, "storageCode cannot be null or the empty string");
        this.storageCode = storageCode;
        Utils.notNullOrEmpty(type, "type cannot be null or the empty string");
        this.type = type;
        if (version < 0) {
            throw new IllegalArgumentException("version must be at least 0");
        }
        this.version = Optional.of(version);
    }
    
    /** Create the correct type instance without needing to check if a version exists.
     * @param type the type name.
     * @param version the version, or null for a type without a version.
     * @return the type instance.
     */
    public static StorageObjectType fromNullableVersion(
            final String storageCode,
            final String type,
            final Integer version) {
        if (version == null) {
            return new StorageObjectType(storageCode, type);
        } else {
            return new StorageObjectType(storageCode, type, version);
        }
    }
    
    /** Get the storage code.
     * @return the storage code.
     */
    public String getStorageCode() {
        return storageCode;
    }

    /** Get the type name.
     * @return the type name.
     */
    public String getType() {
        return type;
    }

    /** Get the version of the type.
     * @return the version of the type, or absent if there is no version.
     */
    public Optional<Integer> getVersion() {
        return version;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StorageObjectType [storageCode=");
        builder.append(storageCode);
        builder.append(", type=");
        builder.append(type);
        builder.append(", version=");
        builder.append(version);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((storageCode == null) ? 0 : storageCode.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
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
        StorageObjectType other = (StorageObjectType) obj;
        if (storageCode == null) {
            if (other.storageCode != null) {
                return false;
            }
        } else if (!storageCode.equals(other.storageCode)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        if (version == null) {
            if (other.version != null) {
                return false;
            }
        } else if (!version.equals(other.version)) {
            return false;
        }
        return true;
    }
    
}
