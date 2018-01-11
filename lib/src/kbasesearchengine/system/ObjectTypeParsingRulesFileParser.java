package kbasesearchengine.system;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.system.ObjectTypeParsingRules.Builder;
import kbasesearchengine.tools.Utils;

/** Utilities for creating {@link ObjectTypeParsingRules} from various data sources.
 * 
 * @author gaprice@lbl.gov
 *
 */
public class ObjectTypeParsingRulesFileParser {

    //TODO TEST

    /** Create a set of ObjectTypeParsingRules version instances from a file.
     * 
     * The rules will have the same storage object type, search type, and ui name.
     * 
     * TODO document the file structure.
     * @param file the file containing the parsing rules.
     * @return a new set of parsing rules ordered by version.
     * @throws IOException if an IO error occurs reading the file.
     * @throws TypeParseException if the file contains erroneous parsing rules.
     */
    public static List<ObjectTypeParsingRules> fromFile(final File file) 
            throws IOException, TypeParseException {
        try (final InputStream is = new FileInputStream(file)) {
            return fromStream(is, file.toString());
        }
    }
    
    /** Create a set of ObjectTypeParsingRules versions from a stream.
     *
     * The rules will have the same storage object type, search type, and ui name.
     *
     * @param is the stream to parse.
     * @param sourceInfo information about the source of the stream, usually a file name.
     * @return a new set of parsing rules ordered by version.
     * @throws IOException if an IO error occurs reading the stream.
     * @throws TypeParseException if the stream contains erroneous parsing rules.
     */
    public List<ObjectTypeParsingRules> parseStream(
            final InputStream is,
            final String sourceInfo)
            throws IOException, TypeParseException {
        return fromStream(is, sourceInfo);
    }

    private static List<ObjectTypeParsingRules> fromStream(InputStream is, String sourceInfo) 
            throws IOException, TypeParseException {
        if (!(is instanceof BufferedInputStream)) {
            is = new BufferedInputStream(is);
        }
        final Yaml yaml = new Yaml(new SafeConstructor());
        final Object predata;
        try {
            predata = yaml.load(is);
        } catch (Exception e) {
            // wtf snakeyaml authors, not using checked exceptions is bad enough, but not
            // documenting any exceptions and overriding toString so you can't tell what
            // exception is being thrown is something else
            throw new TypeParseException(String.format("Error parsing source %s: %s %s",
                    sourceInfo, e.getClass(), e.getMessage()), e);
        }
        if (!(predata instanceof Map)) {
            throw new TypeParseException(
                    "Expected mapping in top level YAML/JSON in source: " + sourceInfo);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) predata;
        return fromObject(obj, sourceInfo);
    }

    //TODO CODE should look at json schema for validating the object data prior to building objects, avoids a lot of typechecking code
    private static List<ObjectTypeParsingRules> fromObject(
            final Map<String, Object> obj,
            final String sourceInfo) 
            throws TypeParseException {
        try {
            final String storageCode = (String)obj.get("storage-type");
            final String type = (String)obj.get("storage-object-type");
            if (Utils.isNullOrEmpty(storageCode)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-type"));
            }
            if (Utils.isNullOrEmpty(type)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-object-type"));
            }
            final StorageObjectType storageType = new StorageObjectType(storageCode, type);
            //TODO CODE better error if missing elements
            final String searchType = (String) obj.get("global-object-type");
            final String uiTypeName = (String) obj.get("ui-type-name");
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> versions =
                    (List<Map<String, Object>>) obj.get("versions");
            final List<ObjectTypeParsingRules> ret = new LinkedList<>();
            for (int i = 0; i < versions.size(); i++) {
                final Builder builder = ObjectTypeParsingRules.getBuilder(
                        new SearchObjectType(searchType, i + 1), storageType)
                        .withNullableUITypeName(uiTypeName);
                ret.add(processVersion(builder, versions.get(i)));
            }
            return ret;
        } catch (ObjectParseException | IllegalArgumentException | NullPointerException e) {
            throw new TypeParseException(String.format("Error in source %s: %s",
                    sourceInfo, e.getMessage()), e);
        }
    }

    private static ObjectTypeParsingRules processVersion(
            final Builder builder,
            final Map<String, Object> versionObj)
            throws ObjectParseException {
        
        final String subType = (String) versionObj.get("inner-sub-type");
        if (!Utils.isNullOrEmpty(subType)) {
            builder.toSubObjectRule(
                    subType,
                    //TODO CODE add checks to ensure these exist
                    getPath((String) versionObj.get("path-to-sub-objects")),
                    getPath((String) versionObj.get("primary-key-path")));
        } // throw exception if the other subobj values exist?
        // Indexing
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> indexingRules =
                (List<Map<String, Object>>) versionObj.get("indexing-rules");
        if (indexingRules != null) {
            for (Map<String, Object> rulesObj : indexingRules) {
                builder.withIndexingRule(buildRule(rulesObj));
            }
        }
        return builder.build();
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
            final Integer targetObjectTypeVersion =
                    (Integer) rulesObj.get("target-object-type-version");
            final String[] tranSplt = transform.split("\\.", 2);
            final String transProp = tranSplt.length == 1 ? null : tranSplt[1];
            irBuilder.withTransform(Transform.unknown(tranSplt[0], transProp,
                    targetObjectType, targetObjectTypeVersion, subObjectIDKey));
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
