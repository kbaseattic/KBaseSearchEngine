package kbasesearchengine.events.handler;

import java.time.Instant;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

public class ResolvedReference {
    
    //TODO JAVADOC
    //TODO TEST
    
    private final GUID reference;
    private final GUID resolvedReference;
    private final StorageObjectType type;
    private final Instant timestamp;
    
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

    public GUID getReference() {
        return reference;
    }

    public GUID getResolvedReference() {
        return resolvedReference;
    }

    public StorageObjectType getType() {
        return type;
    }

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
}
