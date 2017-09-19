package kbasesearchengine.events.handler;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.StorageObjectType;

public class ResolvedReference {
    
    //TODO JAVADOC
    //TODO TEST
    
    private final String reference;
    private final String resolvedReference;
    private final GUID resolvedReferenceAsGUID;
    private final StorageObjectType type;
    private final long timestamp;
    
    public ResolvedReference(
            final String reference,
            final String resolvedReference,
            final GUID resolvedReferenceAsGUID,
            final StorageObjectType type,
            final long timestamp) {
        this.reference = reference;
        this.resolvedReference = resolvedReference;
        this.resolvedReferenceAsGUID = resolvedReferenceAsGUID;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String getReference() {
        return reference;
    }

    public String getResolvedReference() {
        return resolvedReference;
    }

    public GUID getResolvedReferenceAsGUID() {
        return resolvedReferenceAsGUID;
    }

    public StorageObjectType getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ResolvedReference [reference=");
        builder.append(reference);
        builder.append(", resolvedReference=");
        builder.append(resolvedReference);
        builder.append(", resolvedReferenceAsGUID=");
        builder.append(resolvedReferenceAsGUID);
        builder.append(", type=");
        builder.append(type);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append("]");
        return builder.toString();
    }
}
