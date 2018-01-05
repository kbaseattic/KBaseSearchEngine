package kbasesearchengine.system;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import kbasesearchengine.tools.Utils;

/** A type mapping parser for YAML formatted input with a specific document structure.
 * 
 * TODO DOCS documentation of the file structure.
 * @author gaprice@lbl.gov
 *
 */
public class YAMLTypeMappingParser implements TypeMappingParser {
    
    //TODO TEST
    //TODO CODE there's got to be some code to validate the structure of arbitrary java objects, have a look around. Try jsonschema 
    //TODO CODE error checking sucks
    
    private static final String TYPES_PATH = "/types";
    
    @Override
    public Set<TypeMapping> parse(InputStream input, String sourceInfo)
            throws TypeParseException {
        Utils.nonNull(input, "input");
        if (!(input instanceof BufferedInputStream)) {
            input = new BufferedInputStream(input);
        }
        if (Utils.isNullOrEmpty(sourceInfo)) {
            sourceInfo = null;
        }
        final Yaml yaml = new Yaml(new SafeConstructor()); // not thread safe
        final Object predata = yaml.load(input);
        if (!(predata instanceof Map)) {
            throw new TypeParseException(
                    "Expected mapping in top level YAML." + sourceInfo);
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> data = (Map<String, Object>) predata;
        final String storageCode = getString(
                data, "storage-type", "storage-type", sourceInfo);
        final Map<String, Object> types = getStringMap(
                data, "types", TYPES_PATH, sourceInfo);
        final Set<TypeMapping> ret = new HashSet<>();
        for (final String key: types.keySet()) {
            final Map<String, Object> typeinfo = getStringMap(
                    types, key, TYPES_PATH + "/" + key, sourceInfo);
            ret.add(processType(storageCode, key, typeinfo, sourceInfo));
        }
        return ret;
    }

    private TypeMapping processType(
            final String storageCode,
            final String type,
            final Map<String, Object> typeinfo,
            final String sourceInfo)
            throws TypeParseException {
        final TypeMapping.Builder b = TypeMapping.getBuilder(storageCode, type)
                .withNullableSourceInfo(sourceInfo);
        String pathPrefix = TYPES_PATH + "/" + type + "/";
        final List<SearchObjectType> searchTypes = getSearchTypes(
                typeinfo, "types", pathPrefix + "types", sourceInfo, false);
        if (!searchTypes.isEmpty()) {
            searchTypes.stream().forEach(t -> b.withDefaultSearchType(t));
            return b.build();
        }
        getSearchTypes(typeinfo, "default", pathPrefix + "default", sourceInfo, false).stream()
                .forEach(t -> b.withDefaultSearchType(t));
        final String verPathPrefix = pathPrefix + "versions";
        final Map<Integer, Object> versions = getIntMap(
                typeinfo, "versions", verPathPrefix, sourceInfo);
        for (final Integer v: versions.keySet()) {
            final String verpath = verPathPrefix + "/" + v;
            if (v < 0) {
                throw new TypeParseException(String.format(
                        "Version less than 0 at %s.%s", verpath, fmt(sourceInfo)));
            }
            getSearchTypes(versions, v, verpath, sourceInfo, true).stream()
                    .forEach(t -> b.withVersion(v, t));
        }
        if (!b.isBuildReady()) {
            throw new TypeParseException(String.format(
                    "No type mappings provided at %s.%s", pathPrefix, fmt(sourceInfo)));
        }
        return b.build();
    }

    private List<SearchObjectType> getSearchTypes(
            final Map<?, Object> map,
            final Object key,
            final String path,
            final String sourceInfo,
            final boolean required)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null) {
            if (required) {
                throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
            } else {
                return Collections.emptyList();
            }
        }
        if (!(value instanceof List)) {
            throw new TypeParseException("Expected list at " + path + "." + fmt(sourceInfo));
        }
        @SuppressWarnings("unchecked")
        final List<Object> otypes = (List<Object>) value;
        final List<SearchObjectType> ret = new LinkedList<>();
        for (int i = 0; i < otypes.size(); i++) {
            final Object putativeType = otypes.get(i);
            if (!(putativeType instanceof Map)) {
                throw new TypeParseException(String.format(
                        "Expected mapping at %s position %s.%s", path, i, fmt(sourceInfo)));
            }
            @SuppressWarnings("unchecked")
            final Map<Object, Object> putativeType2 = (Map<Object, Object>) putativeType;
            final Object typeName = putativeType2.get("type");
            final Object typeVer = putativeType2.get("version");
            if (typeName == null || !(typeName instanceof String) ||
                    Utils.isNullOrEmpty((String) typeName)) {
                throw new TypeParseException(String.format(
                        "Expected type name at %s/%s/type.%s", path, i, fmt(sourceInfo)));
            }
            if (typeVer == null || !(typeVer instanceof Integer)) {
                throw new TypeParseException(String.format(
                        "Expected type version at %s/%s/version.%s", path, i, fmt(sourceInfo)));
            }
            ret.add(new SearchObjectType((String) typeName, (int) typeVer));
        }
        return ret;
    }
    
    private String fmt(final String sourceInfo) {
        return sourceInfo == null ? "" : 
            sourceInfo.trim().isEmpty() ? "" : " Source: " + sourceInfo;
    }

    // never required
    private Map<Integer, Object> getIntMap(
            final Map<String, Object> map,
            final String key,
            final String path,
            final String sourceInfo)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null) {
            return Collections.emptyMap();
        }
        if (!(value instanceof Map)) {
            throw new TypeParseException(
                    String.format("Expected map, got %s at %s.%s", value, path, fmt(sourceInfo)));
        }
        @SuppressWarnings("unchecked")
        final Map<Object, Object> value2 = (Map<Object, Object>) value;
        final Map<Integer, Object> ret = new HashMap<>();
        for (final Entry<Object, Object> e: value2.entrySet()) {
            if (!(e.getKey() instanceof Integer)) {
                throw new TypeParseException(String.format(
                        "Expected map with int keys, got key %s at %s.%s",
                        e.getKey(), path, fmt(sourceInfo)));
            }
            ret.put((Integer) e.getKey(), e.getValue());
        }
        return ret;
    }
    
    // always required.
    // could probably use generics here, but f it. C&P FTW
    private Map<String, Object> getStringMap(
            final Map<String, Object> map,
            final String key,
            final String path,
            final String sourceInfo)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null) {
            throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
        }
        if (!(value instanceof Map)) {
            throw new TypeParseException(
                    String.format("Expected map, got %s at %s.%s", value, path, fmt(sourceInfo)));
        }
        @SuppressWarnings("unchecked")
        final Map<Object, Object> value2 = (Map<Object, Object>) value;
        final Map<String, Object> ret = new HashMap<>();
        for (final Entry<Object, Object> e: value2.entrySet()) {
            if (!(e.getKey() instanceof String)) {
                throw new TypeParseException(String.format(
                        "Expected map with string keys, got key %s at %s.%s",
                        e.getKey(), path, fmt(sourceInfo)));
            }
            ret.put((String) e.getKey(), e.getValue());
        }
        return ret;
    }

    // always required
    private String getString(
            final Map<?, Object> map,
            final Object key,
            final String path,
            final String sourceInfo)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null) {
            throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
        }
        if (!(value instanceof String)) {
            throw new TypeParseException(
                    String.format("Expected string, got %s at %s.%s",
                            value, path, fmt(sourceInfo)));
        }
        if (Utils.isNullOrEmpty((String) value)) {
            throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
        }
        return (String) value;
    }
    
    public static void main(final String[] args) throws FileNotFoundException, TypeParseException {
        System.out.println(new YAMLTypeMappingParser().parse(
                new FileInputStream(
                        new File("resources/typemappings/GenomeAndAssembly.yaml.example")),
                "me bum"));
    }
}
