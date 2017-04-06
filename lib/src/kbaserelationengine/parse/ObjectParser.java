package kbaserelationengine.parse;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.UObject;
import workspace.GetObjects2Params;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class ObjectParser {
    public void filterSubObjects(URL wsUrl, File tempDir, AuthToken token, 
            String objRef, ObjectTypeParsingRules parsingRules,
            IndexingStorage indexStorage, RelationStorage relationStorage) throws Exception {
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        File tempFile = File.createTempFile("ws_srv_response_", ".json", tempDir);
        WorkspaceClient wc = new WorkspaceClient(wsUrl, token);
        wc.setIsInsecureHttpConnectionAllowed(true);
        wc.setStreamingModeOn(true);
        wc._setFileForNextRpcResponse(tempFile);
        ObjectData obj = wc.getObjects2(new GetObjects2Params().withObjects(
                Arrays.asList(new ObjectSpecification().withRef(objRef)))).getData().get(0);
        String resolvedRef = getRefFromObjectInfo(obj.getInfo());
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (ObjectJsonPath path : parsingRules.getIndexingPathToRules().keySet()) {
            indexingPaths.add(path);
        }
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = obj.getData().getPlacedStream()) {
            ObjectJsonPath pathToSubObjects = parsingRules.getPathToSubObjects() == null ?
                    new ObjectJsonPath("/") : parsingRules.getPathToSubObjects();
            SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
        }
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                        parsingRules.getForeignKeyPathToLookupRules(), subJts, idConsumer);
            }
            String storageType = parsingRules.getStorageType();
            if (storageType == null) {
                storageType = "WS";
            }
            String id = storageType + "/" + resolvedRef;
            String typedId = resolvedRef;
            if (parsingRules.getPathToSubObjects() != null) {
                String innerTextId = idConsumer.getPrimaryKey() == null ? path.toString() : 
                    ("/" + String.valueOf(idConsumer.getPrimaryKey()));
                String innerSubType = parsingRules.getInnerSubType() == null ? "sub" : 
                    parsingRules.getInnerSubType();
                id += "/" + innerSubType + innerTextId;
                typedId += innerTextId;
            }
            String objectType = parsingRules.getGlobalObjectType();
            Object subObjValue = UObject.transformStringToObject(subJson, Object.class);
            indexStorage.indexObject(id, objectType, typedId, subObjValue, 
                    parsingRules.getIndexingPathToRules());
            for (KeyLookupRules lookupRules : idConsumer.getRulesToForeignKeys().keySet()) {
                Set<Object> foreignIds = idConsumer.getRulesToForeignKeys().get(lookupRules);
                for (Object foreignId : foreignIds) {
                    String id2 = indexStorage.lookupIdByTypedId(lookupRules.getTargetObjectType(),
                            String.valueOf(foreignId));
                    relationStorage.addRelation(id, id2, lookupRules.getRelationType());
                }
            }
        }
    }
    
    public static String getRefFromObjectInfo(Tuple11<Long, String, String, String, 
            Long, String, Long, String, String, Long, Map<String,String>> info) {
        return info.getE7() + "/" + info.getE1() + "/" + info.getE5();
    }
}
