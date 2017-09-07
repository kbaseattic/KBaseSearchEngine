package kbaserelationengine.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import kbaserelationengine.tools.Utils;

/** A type mapping parser for YAML formatted input with a specific document structure.
 * 
 * TODO documentation of the structure.
 * @author gaprice@lbl.gov
 *
 */
public class YAMLTypeMappingParser implements TypeMappingParser {
    
    //TODO TEST
    
    private static final String TYPES_PATH = "/types";
    
    @Override
    public Set<TypeMapping> parse(final InputStream input, String sourceInfo)
            throws TypeParseException {
        Utils.nonNull(input, "input");
        if (Utils.isNullOrEmpty(sourceInfo)) {
            sourceInfo = "";
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
                data, "storage-type", "storage-type", sourceInfo, true);
        final Map<String, Object> types = getStringMap(
                data, "types", TYPES_PATH, sourceInfo, true);
        final Set<TypeMapping> ret = new HashSet<>();
        for (final String key: types.keySet()) {
            final Map<String, Object> typeinfo = getStringMap(
                    types, key, TYPES_PATH + "/" + key, sourceInfo, true);
            ret.addAll(processType(storageCode, key, typeinfo, sourceInfo));
        }
        return ret;
    }

    private List<TypeMapping> processType(
            final String storageCode,
            final String type,
            final Map<String, Object> typeinfo,
            final String sourceInfo)
            throws TypeParseException {
        String pathPrefix = TYPES_PATH + "/" + type + "/";
        final String searchType = getString(
                typeinfo, "type", pathPrefix + "type", sourceInfo, false);
        if (searchType != null) {
            return Arrays.asList(new TypeMapping(
                    new StorageObjectType(storageCode, type), searchType, sourceInfo, false));
        }
        final List<TypeMapping> ret = new LinkedList<>();
        final String default_ = getString(
                typeinfo, "default", pathPrefix + "default", sourceInfo, false);
        if (default_ != null) {
            ret.add(new TypeMapping(
                    new StorageObjectType(storageCode, type), default_, sourceInfo, true));
        }
        pathPrefix = pathPrefix + "versions";
        final Map<Integer, Object> versions = getIntMap(
                typeinfo, "versions", pathPrefix, sourceInfo, false);
        for (final Integer v: versions.keySet()) {
            final String verpath = pathPrefix + "/" + v;
            if (v < 0) {
                throw new TypeParseException(String.format(
                        "Version less than 0 at %s.%s", verpath, fmt(sourceInfo)));
            }
            final String searchVerType = getString(
                    versions, v, verpath, sourceInfo, true);
            ret.add(new TypeMapping(new StorageObjectType(storageCode, type, v),
                    searchVerType, sourceInfo, false));
        }
        return ret;
    }

    private String fmt(final String sourceInfo) {
        return sourceInfo.isEmpty() ? "" : " Source: " + sourceInfo;
    }

    private Map<Integer, Object> getIntMap(
            final Map<String, Object> map,
            final String key,
            final String path,
            final String sourceInfo,
            final boolean required)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null && required) {
            throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
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
    
    // could probably use generics here, but f it. C&P FTW
    private Map<String, Object> getStringMap(
            final Map<String, Object> map,
            final String key,
            final String path,
            final String sourceInfo,
            final boolean required)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null && required) {
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

    private String getString(
            final Map<?, Object> map,
            final Object key,
            final String path,
            final String sourceInfo,
            final boolean required)
            throws TypeParseException {
        final Object value = map.get(key);
        if (value == null && required) {
            throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
        }
        if (value != null && !(value instanceof String)) {
            throw new TypeParseException(
                    String.format("Expected string, got %s at %s.%s",
                            value, path, fmt(sourceInfo)));
        }
        String value2 = (String) value;
        if (Utils.isNullOrEmpty(value2)) {
            if (required) {
                throw new TypeParseException("Missing value at " + path + "." + fmt(sourceInfo));
            }
            value2 = null;
        }
        return value2;
    }
    
    public static void main(final String[] args) throws FileNotFoundException, TypeParseException {
        System.out.println(new YAMLTypeMappingParser().parse(
                new FileInputStream(
                        new File("resources/typemappings/GenomeAndAssembly.yaml.example")),
                "me bum"));
    }
}
