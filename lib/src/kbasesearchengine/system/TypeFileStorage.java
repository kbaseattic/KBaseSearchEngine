package kbasesearchengine.system;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;

import kbasesearchengine.main.LineLogger;
import kbasesearchengine.tools.Utils;

/** Flat file based storage for search transformation specifications and
 * storage type / version -> search transformation type / version mappings.
 * 
 * @see ObjectTypeParsingRulesFileParser
 * @see TypeMappingParser
 * @see TypeMapping
 * 
 * @author gaprice@lbl.gov
 *
 */
public class TypeFileStorage implements TypeStorage {
    
    private static final String TYPE_STORAGE = "[TypeStorage]";
    // as opposed to file types for mappings
    private static final Set<String> ALLOWED_FILE_TYPES_FOR_TYPES =
            new HashSet<>(Arrays.asList(".json", ".yaml"));
    
    private final Map<String, ArrayList<ObjectTypeParsingRules>> searchTypes = new HashMap<>();
    private final Map<CodeAndType, TypeMapping> storageTypes;
    
    private Map<CodeAndType, TypeMapping> processTypesDir(
            final Path typesDir,
            final ObjectTypeParsingRulesFileParser searchSpecParser,
            final FileLister fileLister,
            final LineLogger logger)
            throws IOException, TypeParseException {
        final Map<String, Path> typeToFile = new HashMap<>();
        final Map<CodeAndType, TypeMapping.Builder> storageTypes = new HashMap<>(); 
        for (final Path file: fileLister.list(typesDir)) {
            if (fileLister.isRegularFile(file) && isAllowedFileType(file)) {
                final List<ObjectTypeParsingRules> types;
                try (final InputStream is = fileLister.newInputStream(file)) {
                    types = searchSpecParser.parseStream(is, file.toString());
                }
                final String searchType = types.get(0).getGlobalObjectType().getType();
                if (typeToFile.containsKey(searchType)) {
                    throw new TypeParseException(String.format(
                            "Multiple definitions for the same search type %s in files %s and %s",
                            searchType, file, typeToFile.get(searchType)));
                }
                typeToFile.put(searchType, file);
                searchTypes.put(searchType, new ArrayList<>(types));
                final CodeAndType cnt = new CodeAndType(types.get(0).getStorageObjectType());
                if (!storageTypes.containsKey(cnt)) {
                    storageTypes.put(cnt, TypeMapping.getBuilder(cnt.storageCode, cnt.storageType)
                            .withDefaultSearchType(new SearchObjectType(searchType, types.size()))
                            .withNullableSourceInfo(file.toString()));
                } else {
                    storageTypes.get(cnt)
                            .withDefaultSearchType(new SearchObjectType(searchType, types.size()));
                }
                logger.logInfo(String.format("%s Processed type tranformation file with storage " +
                        "code %s, storage type %s and search type %s: %s",
                        TYPE_STORAGE, cnt.storageCode, cnt.storageType, searchType, file));
            } else {
                logger.logInfo(TYPE_STORAGE + " Skipping file in type tranformation directory: " +
                        file);
            }
        }
        final Map<CodeAndType, TypeMapping> ret = new HashMap<>();
        storageTypes.keySet().stream().forEach(k -> ret.put(k, storageTypes.get(k).build()));
        return ret;
    }

    private boolean isAllowedFileType(final Path file) {
        final String path = file.toString();
        for (final String allowedExtension: ALLOWED_FILE_TYPES_FOR_TYPES) {
            if (path.endsWith(allowedExtension)) {
                return true;
            }
        }
        return false;
    }
    
    private static class CodeAndType {
        private final String storageCode;
        private final String storageType;
        
        private CodeAndType(final TypeMapping type) {
            this.storageCode = type.getStorageCode();
            this.storageType = type.getStorageType();
        }
        
        private CodeAndType(final StorageObjectType type) {
            this.storageCode = type.getStorageCode();
            this.storageType = type.getType();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + storageCode.hashCode();
            result = prime * result + storageType.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            CodeAndType other = (CodeAndType) obj;
            if (!storageCode.equals(other.storageCode)) {
                return false;
            }
            if (!storageType.equals(other.storageType)) {
                return false;
            }
            return true;
        }
    }
    
    // could make a simpler constructor with default args for the parsers and lister
    /** Create a new type storage system.
     * @param typesDir the directory in which to find transformation specifications.
     * @param mappingsDir the directory in which to find type mappings.
     * @param searchSpecParser the parser for transformation specifications.
     * @param mappingParsers one or more parsers for type mappings. The map maps from file
     * extension (e.g. "yaml") to mapper implementation.
     * @param fileLister a file handler instance.
     * @param logger a logger.
     * @throws IOException if errors occur when reading a file.
     * @throws TypeParseException if a file could not be parsed.
     */
    public TypeFileStorage(
            final Path typesDir,
            final Path mappingsDir,
            final ObjectTypeParsingRulesFileParser searchSpecParser,
            final Map<String, TypeMappingParser> mappingParsers,
            final FileLister fileLister,
            final LineLogger logger)
            throws IOException, TypeParseException {
        Utils.nonNull(typesDir, "typesDir");
        Utils.nonNull(mappingsDir, "mappingsDir");
        Utils.nonNull(searchSpecParser, "searchSpecParser");
        Utils.nonNull(mappingParsers, "mappingParsers");
        Utils.nonNull(fileLister, "fileLister");
        Utils.nonNull(logger, "logger");
        storageTypes = processTypesDir(typesDir, searchSpecParser, fileLister, logger);
        final Map<CodeAndType, TypeMapping> mappings = processMappingsDir(
                mappingsDir, mappingParsers, fileLister, logger);
        for (final CodeAndType cnt: mappings.keySet()) {
            if (storageTypes.containsKey(cnt)) {
                final String mappingSource = mappings.get(cnt).getSourceInfo().orNull();
                logger.logInfo(String.format(
                        "%s Overriding type mapping for storage code %s and storage type %s " +
                        "from type transformation file with definition from type mapping file%s",
                        TYPE_STORAGE, cnt.storageCode, cnt.storageType,
                        mappingSource == null ? "" : " " + mappingSource));
            } // ok to set up a mapping for a storage type not explicitly listed in a search
              // type file, so we don't throw an exception here
            storageTypes.put(cnt, mappings.get(cnt));
        }
    }
    
    private Map<CodeAndType, TypeMapping> processMappingsDir(
            final Path mappingsDir,
            final Map<String, TypeMappingParser> parsers,
            final FileLister fileLister,
            final LineLogger logger)
            throws IOException, TypeParseException {
        final Map<CodeAndType, TypeMapping> ret = new HashMap<>();
        for (final Path file : fileLister.list(mappingsDir)) {
            if (fileLister.isRegularFile(file)) {
                final String ext = FilenameUtils.getExtension(file.toString());
                final TypeMappingParser parser = parsers.get(ext);
                if (parser != null) {
                    final Set<TypeMapping> mappings;
                    try (final InputStream is = fileLister.newInputStream(file)) {
                        mappings = parser.parse(is, file.toString());
                    }
                    for (final TypeMapping map: mappings) {
                        final CodeAndType cnt = new CodeAndType(map);
                        if (ret.containsKey(cnt)) {
                            throw typeMappingCollisionException(map, ret.get(cnt));
                        }
                        final String source = map.getSourceInfo().orNull();
                        for (final SearchObjectType searchType: map.getSearchTypes()) {
                            if (!searchTypes.containsKey(searchType.getType())) {
                                throw new TypeParseException(String.format(
                                        "The search type %s specified in source code/type %s/%s " +
                                        "does not have an equivalent transform type.%s",
                                        searchType.getType(), cnt.storageCode, cnt.storageType,
                                        source == null ? "" : " File: " + source));
                            }
                            if (searchTypes.get(searchType.getType()).size() <
                                    searchType.getVersion()) {
                                throw new TypeParseException(String.format(
                                        "Version %s of search type %s specified in " +
                                        "source code/type %s/%s does not exist.%s",
                                        searchType.getVersion(), searchType.getType(),
                                        cnt.storageCode, cnt.storageType,
                                        source == null ? "" : " File: " + source));
                            }
                        }
                        ret.put(cnt, map);
                    }
                    final String source = mappings.iterator().next().getSourceInfo().orNull();
                    logger.logInfo(String.format(TYPE_STORAGE +
                            " Processed type mapping file with storage code %s and types %s.%s",
                            mappings.iterator().next().getStorageCode(),
                            String.join(", ", mappings.stream().map(m -> m.getStorageType())
                                    .sorted().collect(Collectors.toList())),
                            source == null ? "" : " File: " + source));
                } else {
                    logger.logInfo(TYPE_STORAGE + " Skipping file in type mapping directory: " +
                            file);
                }
            } else {
                logger.logInfo(TYPE_STORAGE +
                        " Skipping entry in type mapping directory: " + file);
            }
        }
        return ret;
    }

    private TypeParseException typeMappingCollisionException(
            final TypeMapping map,
            final TypeMapping priorMapping) {
        final String source = map.getSourceInfo().orNull();
        final String priorSource = priorMapping.getSourceInfo().orNull();
        String exception = String.format("Type collision for type %s in storage %s.",
                map.getStorageType(), map.getStorageCode());
        final List<String> files = new LinkedList<>();
        if (source != null) {
            files.add(source);
        }
        if (priorSource != null) {
            files.add(priorSource);
        }
        if (!files.isEmpty()) {
            Collections.sort(files);
            exception += " (" + String.join(", ", files) + ")";
        }
        return new TypeParseException(exception);
    }

    @Override
    public Set<ObjectTypeParsingRules> listObjectTypeParsingRules() {
        return searchTypes.values().stream().map(l -> l.get(l.size() - 1))
                .collect(Collectors.toSet());
    }
    
    @Override
    public ObjectTypeParsingRules getObjectTypeParsingRules(final SearchObjectType type)
            throws NoSuchTypeException {
        //TODO CODE seems like throwing an error here for the guid transform case is a late fail. The check should occur when the OTPRs are being built.
        if (searchTypes.containsKey(type.getType())) {
            final ArrayList<ObjectTypeParsingRules> vers = searchTypes.get(type.getType());
            if (type.getVersion() > vers.size()) {
                throw new NoSuchTypeException(String.format("No type %s_%s found",
                        type.getType(), type.getVersion()));
            }
            return vers.get(type.getVersion() - 1);
        } else {
            throw new NoSuchTypeException(String.format("No type %s_%s found",
                    type.getType(), type.getVersion()));
        }
    }
    
    @Override
    public Set<ObjectTypeParsingRules> listObjectTypeParsingRules(
            final StorageObjectType storageObjectType) {
        final TypeMapping mapping = storageTypes.get(new CodeAndType(storageObjectType));
        if (mapping == null) {
            return Collections.emptySet();
        }
        final Set<SearchObjectType> types = mapping.getSearchTypes(storageObjectType.getVersion());
        final Set<ObjectTypeParsingRules> ret = new HashSet<>();
        for (final SearchObjectType t: types) {
            ret.add(searchTypes.get(t.getType()).get(t.getVersion() - 1));
        }
        return ret;
    }
}
