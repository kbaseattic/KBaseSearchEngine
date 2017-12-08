package kbasesearchengine.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.system.ObjectTypeParsingRules.Builder;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

/** Utilities for creating {@link ObjectTypeParsingRules} from various data sources.
 * @author gaprice@lbl.gov
 *
 */
public class ObjectTypeParsingRulesUtils {

    //TODO CODE not sure if throwing ObjectParseException makes sense here
    //TODO TEST

    /** Create an ObjectTypeParsingRules instance from a file.
     * 
     * TODO document the file structure.
     * @param file the file containing the parsing rules.
     * @return a new set of parsing rules.
     * @throws ObjectParseException if the file contains erroneous parsing rules.
     * @throws IOException if an IO error occurs reading the file.
     */
    public static ObjectTypeParsingRules fromFile(final File file) 
            throws ObjectParseException, IOException {
        try (final InputStream is = new FileInputStream(file)) {
            return fromStream(is, file.toString());
        }
    }

    private static ObjectTypeParsingRules fromStream(InputStream is, String sourceInfo) 
            throws IOException, ObjectParseException {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = UObject.getMapper().readValue(is, Map.class);
        return fromObject(obj, sourceInfo);
    }

    private static ObjectTypeParsingRules fromObject(
            final Map<String, Object> obj,
            final String sourceInfo) 
            throws ObjectParseException {
        try {
            final String storageCode = (String)obj.get("storage-type");
            final String type = (String)obj.get("storage-object-type");
            if (Utils.isNullOrEmpty(storageCode)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-type"));
            }
            if (Utils.isNullOrEmpty(type)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-object-type"));
            }
            final Builder builder = ObjectTypeParsingRules.getBuilder(
                    (String) obj.get("global-object-type"), //TODO CODE better error if missing
                    new StorageObjectType(storageCode, type))
                    .withNullableUITypeName((String)obj.get("ui-type-name"));
            final String subType = (String)obj.get("inner-sub-type");
            if (!Utils.isNullOrEmpty(subType)) {
                builder.toSubObjectRule(
                        subType,
                        //TODO CODE add checks to ensure these exist
                        getPath((String)obj.get("path-to-sub-objects")),
                        getPath((String)obj.get("primary-key-path")));
            } // throw exception if the other subobj values exist?
            // Indexing
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> indexingRules =
                    (List<Map<String, Object>>)obj.get("indexing-rules");
            if (indexingRules != null) {
                for (Map<String, Object> rulesObj : indexingRules) {
                    builder.withIndexingRule(buildRule(rulesObj));
                }
            }
            return builder.build();
        } catch (ObjectParseException | IllegalArgumentException | NullPointerException e) {
            throw new ObjectParseException(String.format("Error in source %s: %s",
                    sourceInfo, e.getMessage()), e);
        }
    }

    private static IndexingRules buildRule(
            final Map<String, Object> rulesObj)
            throws ObjectParseException {
        final String path = (String) rulesObj.get("path");
        final String keyName = (String) rulesObj.get("key-name");
        final IndexingRules.Builder irBuilder;
        if (Utils.isNullOrEmpty(path)) {
            final String sourceKey = (String)rulesObj.get("source-key");
            irBuilder = IndexingRules.fromSourceKey(sourceKey, keyName);
        } else {
            //TODO CODE throw exception if sourceKey != null?
            irBuilder = IndexingRules.fromPath(new ObjectJsonPath(path));
            if (!Utils.isNullOrEmpty(keyName)) {
                irBuilder.withKeyName(keyName);
            }
        }
        if (getBool((Boolean) rulesObj.get("from-parent"))) {
            //TODO NNOW throw exception if not a sub type
            irBuilder.withFromParent();
        }
        if (getBool(rulesObj.get("full-text"))) {
            irBuilder.withFullText();
        }
        final String keywordType = (String)rulesObj.get("keyword-type");
        if (!Utils.isNullOrEmpty(keywordType)) {
            //TODO CODE throw an error if fullText is true?
            irBuilder.withKeywordType(keywordType);
        }
        final String transform = (String) rulesObj.get("transform");
        if (!Utils.isNullOrEmpty(transform)) {
            final String subObjectIDKey = (String) rulesObj.get("subobject-id-key");
            final String targetObjectType =
                    (String) rulesObj.get("target-object-type");
            final String[] tranSplt = transform.split("\\.", 2);
            final String transProp = tranSplt.length == 1 ? null : tranSplt[1];
            irBuilder.withTransform(Transform.unknown(
                    tranSplt[0], transProp, targetObjectType, subObjectIDKey));
        }
        if (getBool(rulesObj.get("not-indexed"))) {
            irBuilder.withNotIndexed();
        }
        irBuilder.withNullableDefaultValue(rulesObj.get("optional-default-value"));
        irBuilder.withNullableUIName((String) rulesObj.get("ui-name"));
        if (getBool(rulesObj.get("ui-hidden"))) {
            irBuilder.withUIHidden();
        }
        irBuilder.withNullableUILinkKey((String) rulesObj.get("ui-link-key"));
        return irBuilder.build();
    }
    
    private static boolean getBool(final Object putativeBool) {
        //TODO CODE precheck cast exception
        return putativeBool != null && (Boolean) putativeBool; 
    }
    
    private static String getMissingKeyParseMessage(final String key) {
        return String.format("Missing key %s", key);
    }

    private static ObjectJsonPath getPath(String path) throws ObjectParseException {
        return path == null ? null : new ObjectJsonPath(path);
    }
    
}
