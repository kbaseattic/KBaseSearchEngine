package kbaserelationengine.system;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.parse.ObjectParseException;

public class DefaultSystemStorage implements SystemStorage {
    
    //TODO JAVADOC
    //TODO TEST
    
    private final Map<String, ObjectTypeParsingRules> searchTypes = new HashMap<>();
    private final Map<CodeAndType, TypeMapping> storageTypes;
    
    private Map<CodeAndType, TypeMapping> processTypesDir(final Path typesDir)
            throws IOException, ObjectParseException {
        final Map<CodeAndType, TypeMapping.Builder> storageTypes = new HashMap<>(); 
        // this is gross, but works. https://stackoverflow.com/a/20130475/643675
        for (Path file : (Iterable<Path>) Files.list(typesDir)::iterator) {
            if (Files.isRegularFile(file) && file.toString().endsWith(".json")) {
                final ObjectTypeParsingRules type = ObjectTypeParsingRules.fromFile(file.toFile());
                searchTypes.put(type.getGlobalObjectType(), type);
                final CodeAndType cnt = new CodeAndType(
                        type.getStorageObjectType().getStorageCode(),
                        type.getStorageObjectType().getType());
                if (!storageTypes.containsKey(cnt)) {
                    storageTypes.put(cnt, TypeMapping.getBuilder(cnt.storageCode, cnt.storageType)
                            .withNullableDefaultSearchType(type.getGlobalObjectType()));
                } else {
                    storageTypes.get(cnt)
                            .withNullableDefaultSearchType(type.getGlobalObjectType());
                }
            }
        }
        final Map<CodeAndType, TypeMapping> ret = new HashMap<>();
        storageTypes.keySet().stream().forEach(k -> ret.put(k, storageTypes.get(k).build()));
        return ret;
    }
    
    private static class CodeAndType {
        private final String storageCode;
        private final String storageType;
        
        private CodeAndType(String storageCode, String storageType) {
            this.storageCode = storageCode;
            this.storageType = storageType;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("CodeAndType [storageCode=");
            builder.append(storageCode);
            builder.append(", storageType=");
            builder.append(storageType);
            builder.append("]");
            return builder.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((storageCode == null) ? 0 : storageCode.hashCode());
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
            CodeAndType other = (CodeAndType) obj;
            if (storageCode == null) {
                if (other.storageCode != null) {
                    return false;
                }
            } else if (!storageCode.equals(other.storageCode)) {
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
    
    public DefaultSystemStorage(
            final Path typesDir,
            final Path mappingsDir,
            final Map<String, TypeMappingParser> parsers)
            throws IOException, ObjectParseException {
        storageTypes = processTypesDir(typesDir);
        // TODO NOW Auto-generated constructor stub
    }
    
    @Override
    public List<ObjectTypeParsingRules> listObjectTypes() throws IOException {
        return new LinkedList<>(searchTypes.values());
    }
    
    @Override
    public ObjectTypeParsingRules getObjectType(final String type)
            throws IOException {
        if (searchTypes.containsKey(type)) {
            return searchTypes.get(type);
        } else {
            return null;
        }
    }
    
    @Override
    public List<ObjectTypeParsingRules> listObjectTypesByStorageObjectType(
            final StorageObjectType storageObjectType) {
        final TypeMapping mapping = storageTypes.get(
                new CodeAndType(storageObjectType.getStorageCode(), storageObjectType.getType()));
        if (mapping == null) {
            return Collections.emptyList();
        }
        final Set<String> types = mapping.getSearchTypes(storageObjectType.getVersion());
        final List<ObjectTypeParsingRules> ret = new LinkedList<>();
        for (final String t: types) {
            ret.add(searchTypes.get(t));
        }
        return ret;
    }
}
