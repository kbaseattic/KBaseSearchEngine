package kbaserelationengine.parse;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.relations.Relation;
import kbaserelationengine.relations.RelationStorage;
import kbaserelationengine.system.RelationRules;
import kbaserelationengine.system.IndexingRules;
import kbaserelationengine.system.ObjectTypeParsingRules;
import kbaserelationengine.system.SystemStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.UObject;
import workspace.GetObjects2Params;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class ObjectParser {
    
    public static File prepareTempFile(File tempDir) throws IOException {
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        File tempFile = File.createTempFile("ws_srv_response_", ".json", tempDir);
        return tempFile;
    }

    public static ObjectData loadObject(URL wsUrl, File tempFile, AuthToken token,
            String objRef) throws IOException, JsonClientException {
        WorkspaceClient wc = new WorkspaceClient(wsUrl, token);
        wc.setIsInsecureHttpConnectionAllowed(true);
        wc.setStreamingModeOn(true);
        wc._setFileForNextRpcResponse(tempFile);
        return wc.getObjects2(new GetObjects2Params().withObjects(
                Arrays.asList(new ObjectSpecification().withRef(objRef)))).getData().get(0);
    }
    
    public static Map<GUID, String> parseSubObjects(ObjectData obj, String objRef, 
            ObjectTypeParsingRules parsingRules, SystemStorage system,
            RelationStorage relationStorage) throws IOException, ObjectParseException {
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = obj.getData().getPlacedStream()) {
            extractSubObjects(parsingRules, subObjConsumer, jts);
        }
        Map<GUID, String> guidToJson = new LinkedHashMap<>();
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getPrimaryKeyPath() != null || 
                    parsingRules.getRelationRules() != null) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                            parsingRules.getRelationRules(), subJts, idConsumer);
                }
            }
            GUID id = prepareGUID(parsingRules, objRef, path, idConsumer);
            guidToJson.put(id, subJson);
            //storeRelations(parsingRules, system, relationStorage, idConsumer, id);
        }
        return guidToJson;
    }

    public static void storeRelations(ObjectTypeParsingRules parsingRules,
            SystemStorage system, RelationStorage relationStorage,
            SimpleIdConsumer idConsumer, GUID id) throws IOException {
        for (RelationRules lookupRules : idConsumer.getRulesToForeignKeys().keySet()) {
            Set<Object> foreignIds = idConsumer.getRulesToForeignKeys().get(lookupRules);
            Set<GUID> normedIds = system.normalizeObjectIds(foreignIds, 
                    lookupRules.getTargetObjectType());
            List<Relation> links = new ArrayList<>();
            for (GUID id2 : normedIds) {
                Relation link = new Relation();
                link.setId1(id);
                link.setType1(parsingRules.getGlobalObjectType());
                link.setId2(id2);
                link.setType2(lookupRules.getTargetObjectType());
                link.setLinkType(lookupRules.getRelationType());
            }
            relationStorage.addRelations(links);
        }
    }

    public static GUID prepareGUID(ObjectTypeParsingRules parsingRules,
            String resolvedRef, ObjectJsonPath path,
            SimpleIdConsumer idConsumer) {
        String storageType = parsingRules.getStorageType();
        if (storageType == null) {
            storageType = "WS";
        }
        String textId = storageType + ":" + resolvedRef;
        if (parsingRules.getPathToSubObjects() != null) {
            String innerTextId = idConsumer.getPrimaryKey() == null ? path.toString() : 
                ("/" + String.valueOf(idConsumer.getPrimaryKey()));
            String innerSubType = parsingRules.getInnerSubType() == null ? "_" : 
                parsingRules.getInnerSubType();
            textId += ":" + innerSubType + innerTextId;
        }
        GUID id = new GUID(textId);
        return id;
    }
    
    public static String extractParentFragment(ObjectTypeParsingRules parsingRules,
            JsonParser jts) throws ObjectParseException, IOException {
        if (parsingRules.getPathToSubObjects() == null) {
            return null;
        }
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (!rules.isFromParent()) {
                continue;
            }
            indexingPaths.add(rules.getPath());
        }
        if (indexingPaths.size() == 0) {
            return null;
        }
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer parentConsumer = new SimpleSubObjectConsumer(pathToJson);
        ObjectJsonPath pathToSubObjects = new ObjectJsonPath("/");
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, parentConsumer);
        return pathToJson.get(pathToJson.keySet().iterator().next());
    }

    public static void extractSubObjects(ObjectTypeParsingRules parsingRules,
            SubObjectConsumer subObjConsumer, JsonParser jts)
            throws ObjectParseException, IOException {
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (rules.isFromParent()) {
                continue;
            }
            if (rules.getPath() != null) {
                indexingPaths.add(rules.getPath());
            }
        }
        ObjectJsonPath pathToSubObjects = parsingRules.getPathToSubObjects() == null ?
                new ObjectJsonPath("/") : parsingRules.getPathToSubObjects();
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
    }

    public static String getRefFromObjectInfo(Tuple11<Long, String, String, String, 
            Long, String, Long, String, String, Long, Map<String,String>> info) {
        return info.getE7() + "/" + info.getE1() + "/" + info.getE5();
    }
}
