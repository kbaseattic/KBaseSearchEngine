package kbasesearchengine.common;

import java.util.regex.Pattern;

import kbasesearchengine.tools.Utils;

/**
 * A GUID that uniquely identifies an object or an element within an object
 * (a sub-object). The format is,
 *
 *  {storageCode}:[{AccessGroupID}/]{accessGroupObjectId}[/{version}][:{subObjectType}/{subObjectId}]
 *
 *  where,
 *  storageCode is the code of the datasource in which the object resides.
 *
 */
public class GUID {
    
    //TODO JAVADOC
    //TODO TEST
    //TODO CODE check inputs valid
    
    private String storageCode;
    private Integer accessGroupId;
    private String accessGroupObjectId;
    private Integer version;
    private String subObjectType;
    private String subObjectId;
    //
    private static Pattern slashDiv = Pattern.compile(Pattern.quote("/"));
    
    public GUID(String storageCode, Integer accessGroupId, String accessGroupObjectId,
            Integer version, String subObjectType, String subObjectId) {
        this.storageCode = storageCode;
        this.accessGroupId = accessGroupId;
        this.accessGroupObjectId = accessGroupObjectId;
        this.version = version;
        this.subObjectType = subObjectType;
        this.subObjectId = subObjectId;
    }
    
    public GUID(final GUID parentGUID, final String subObjectType, final String subObjectID) {
        storageCode = parentGUID.getStorageCode();
        accessGroupId = parentGUID.getAccessGroupId();
        accessGroupObjectId = parentGUID.getAccessGroupObjectId();
        version = parentGUID.getVersion();
        this.subObjectType = subObjectType;
        this.subObjectId = subObjectID;
    }
    
    public String getStorageCode() {
        return storageCode;
    }
    
    public Integer getAccessGroupId() {
        return accessGroupId;
    }
    
    public String getAccessGroupObjectId() {
        return accessGroupObjectId;
    }
    
    public Integer getVersion() {
        return version;
    }
    
    public String getSubObjectType() {
        return subObjectType;
    }
    
    public String getSubObjectId() {
        return subObjectId;
    }
    
    public static GUID fromRef(final String storageCode, final String ref) {
        Utils.notNullOrEmpty(storageCode, "storageCode cannot be null or empty");
        Utils.notNullOrEmpty(ref, "ref cannot be null or empty");
        if (ref.startsWith(storageCode + ":")) {
            return new GUID(ref);
        } else {
            return new GUID(storageCode + ":" + ref);
        }
    }
    
    public GUID(String textGUID) {
        String innerId = textGUID;
        int colonPos = innerId.indexOf(':');
        if (colonPos <= 0) {
            throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
        }
        this.storageCode = innerId.substring(0, colonPos);
        innerId = innerId.substring(colonPos + 1);
        colonPos = innerId.indexOf(':');
        if (colonPos == 0 || colonPos + 1 == innerId.length()) {
            throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
        }
        String storageObjId = colonPos < 0 ? innerId : innerId.substring(0, colonPos);
        String[] storageObjIdParts = slashDiv.split(storageObjId);
        if (storageObjIdParts.length == 0 || storageObjIdParts.length > 3) {
            throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
        }
        if (storageObjIdParts.length > 1) {
            try {
                this.accessGroupId = Integer.parseInt(storageObjIdParts[0]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
            }
            this.accessGroupObjectId = storageObjIdParts[1];
            //TODO NOW not a guid if version not available...
            if (storageObjIdParts.length == 3) {
                try {
                    this.version = Integer.parseInt(storageObjIdParts[2]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
                }
            }
        } else {
            this.accessGroupObjectId = storageObjIdParts[0];
        }
        //
        if (colonPos > 0) {
            String subObjectPart = innerId.substring(colonPos + 1);
            int slashPos = subObjectPart.indexOf('/');
            if (slashPos == 0 || slashPos + 1 == subObjectPart.length()) {
                throw new IllegalArgumentException("Wrong format for GUID: " + textGUID);
            }
            this.subObjectType = subObjectPart.substring(0, slashPos);
            this.subObjectId = subObjectPart.substring(slashPos + 1);
        }
    }
    
    //TODO NOW if the version or accessGroupID is missing, how do you know which is which? Need to know storage code -> has access group mapping or have a placeholder (-1) for missing access group
    @Override
    public String toString() {
        String objRefId = this.version == null ? 
                ((this.accessGroupId == null ? "" : (this.accessGroupId + "/")) + this.accessGroupObjectId) :
                    (this.accessGroupId + "/" + this.accessGroupObjectId + "/" + this.version);
        return (this.storageCode + ":") + objRefId +
                (this.subObjectType == null ? "" : (":" + this.getSubObjectType() + "/" + this.getSubObjectId()));
    }
    
    public String toRefString() {
        return this.version == null ? 
                ((this.accessGroupId == null ? "" : (this.accessGroupId + "/")) + this.accessGroupObjectId) :
                    (this.accessGroupId + "/" + this.accessGroupObjectId + "/" + this.version);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((accessGroupId == null) ? 0 : accessGroupId.hashCode());
        result = prime * result + ((accessGroupObjectId == null) ? 0
                : accessGroupObjectId.hashCode());
        result = prime * result
                + ((storageCode == null) ? 0 : storageCode.hashCode());
        result = prime * result
                + ((subObjectId == null) ? 0 : subObjectId.hashCode());
        result = prime * result
                + ((subObjectType == null) ? 0 : subObjectType.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GUID other = (GUID) obj;
        if (accessGroupId == null) {
            if (other.accessGroupId != null)
                return false;
        } else if (!accessGroupId.equals(other.accessGroupId))
            return false;
        if (accessGroupObjectId == null) {
            if (other.accessGroupObjectId != null)
                return false;
        } else if (!accessGroupObjectId.equals(other.accessGroupObjectId))
            return false;
        if (storageCode == null) {
            if (other.storageCode != null)
                return false;
        } else if (!storageCode.equals(other.storageCode))
            return false;
        if (subObjectId == null) {
            if (other.subObjectId != null)
                return false;
        } else if (!subObjectId.equals(other.subObjectId))
            return false;
        if (subObjectType == null) {
            if (other.subObjectType != null)
                return false;
        } else if (!subObjectType.equals(other.subObjectType))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }
    
    
}
