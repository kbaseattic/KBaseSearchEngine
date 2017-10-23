package kbasesearchengine.events.handler;

import java.time.Instant;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

/** A reference to an object in an external storage system, and the resolved version of that
 * reference as provided by the external storage system. In some cases the resolved references
 * may be identical to the original reference.
 * @author gaprice@lbl.gov
 *
 */
/**
 * @author gaprice@lbl.gov
 *
 */
public class ResolvedReference {
    
    //TODO CODE check that all storage codes (in GUIDs and SOT) are the same
    
    private final GUID reference;
    private final GUID resolvedReference;
    private final StorageObjectType type;
    private final Instant timestamp;
    
    /** Create a resolved reference.
     * @param reference the original reference.
     * @param resolvedReference the resolved reference.
     * @param type the type of the object to which the reference refers.
     * @param time the timestamp on the object to which the reference refers.
     */
    public ResolvedReference(
            final GUID reference,
            final GUID resolvedReference,
            final StorageObjectType type,
            final Instant time) {
        Utils.nonNull(reference, "reference");
        Utils.nonNull(resolvedReference, "resolvedReference");
        Utils.nonNull(type, "type");
        Utils.nonNull(time, "time");
        this.reference = reference;
        this.resolvedReference = resolvedReference;
        this.type = type;
        this.timestamp = time;
    }

    /** Get the original reference to the data object.
     * @return the original reference.
     */
    public GUID getReference() {
        return reference;
    }

    /** Get the resolved reference to the data object.
     * @return the resolved reference.
     */
    public GUID getResolvedReference() {
        return resolvedReference;
    }

    /** Get the type of the referenced data object.
     * @return the type.
     */
    public StorageObjectType getType() {
        return type;
    }

    /** Get the timestamp on the referenced data object.
     * @return the timestamp.
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ResolvedReference [reference=");
        builder.append(reference);
        builder.append(", resolvedReference=");
        builder.append(resolvedReference);
        builder.append(", type=");
        builder.append(type);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((reference == null) ? 0 : reference.hashCode());
        result = prime * result + ((resolvedReference == null) ? 0
                : resolvedReference.hashCode());
        result = prime * result
                + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        ResolvedReference other = (ResolvedReference) obj;
        if (reference == null) {
            if (other.reference != null) {
                return false;
            }
        } else if (!reference.equals(other.reference)) {
            return false;
        }
        if (resolvedReference == null) {
            if (other.resolvedReference != null) {
                return false;
            }
        } else if (!resolvedReference.equals(other.resolvedReference)) {
            return false;
        }
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        return true;
    }
}
