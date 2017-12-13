package kbasesearchengine.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.LocationTransformType;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.Transform;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

public class KeywordParser {
    
    //TODO EXP handle all exceptions
    
    public static ParsedObject extractKeywords(
            final String type,
            final String json,
            final String parentJson,
            final List<IndexingRules> indexingRules, 
            final ObjectLookupProvider lookup,
            final List<GUID> objectRefPath)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {

        // check pre-conditons
        Utils.notNullOrEmpty(json, "json is a required parameter");

        Utils.nonNull(indexingRules, "indexingRules is a required parameter");

        Map<String, InnerKeyValue> keywords = new LinkedHashMap<>();
        ValueConsumer<List<IndexingRules>> consumer = new ValueConsumer<List<IndexingRules>>() {
            @Override
            public void addValue(List<IndexingRules> rulesList, Object value)
                    throws IndexingException, InterruptedException, ObjectParseException {
                for (IndexingRules rule : rulesList) {
                    processRule(type, rule, rule.getKeyName(), value, keywords, lookup, 
                            objectRefPath);
                }
            }
        };
        // Sub-objects
        extractIndexingPart(json, false, indexingRules, consumer);
        // Parent
        if (parentJson != null) {
            extractIndexingPart(parentJson, true, indexingRules, consumer);
        }
        Map<String, List<IndexingRules>> ruleMap = indexingRules.stream().collect(
                Collectors.groupingBy(rule -> rule.getKeyName()));
        for (String key : ruleMap.keySet()) {
            for (IndexingRules rule : ruleMap.get(key)) {
                if (!rule.isDerivedKey()) {
                    // Let's check that not derived keywords are all set (with optional defaults)
                    List<Object> values = keywords.containsKey(key) ? keywords.get(key).values : 
                        null;
                    if (isEmpty(values)) {
                        processRule(type, rule, key, null, keywords, lookup, objectRefPath);
                    }
                }
            }
        }
        for (String key : ruleMap.keySet()) {
            for (IndexingRules rule : ruleMap.get(key)) {
                if (rule.isDerivedKey()) {
                    processDerivedRule(type, key, rule, ruleMap, keywords, lookup, 
                            new LinkedHashSet<>(), objectRefPath);
                }
            }
        }
        ParsedObject ret = new ParsedObject();
        ret.json = json;
        ret.keywords = keywords.entrySet().stream().filter(kv -> !kv.getValue().notIndexed)
                .collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue().values));
        return ret;
    }

    private static List<Object> processDerivedRule(String type, 
            Map<String, List<IndexingRules>> ruleMap, String key, 
            Map<String, InnerKeyValue> keywords, ObjectLookupProvider lookup, 
            Set<String> keysWaitingInStack, List<GUID> callerRefPath)
            throws IndexingException, InterruptedException, ObjectParseException {
        if (!ruleMap.containsKey(key)) {
            throw new IllegalStateException("Unknown source-key in derived keywords: " + 
                    type + "/" + key);
        }
        List<Object> ret = null;
        for (IndexingRules rule : ruleMap.get(key)) {
            ret = processDerivedRule(type, key, rule, ruleMap, keywords, lookup, 
                    keysWaitingInStack, callerRefPath);
        }
        return ret;
    }
    
    private static List<Object> processDerivedRule(String type, String key, IndexingRules rule,
            Map<String, List<IndexingRules>> ruleMap, Map<String, InnerKeyValue> keywords, 
            ObjectLookupProvider lookup, Set<String> keysWaitingInStack,
            List<GUID> objectRefPath)
            throws IndexingException, InterruptedException, ObjectParseException {
        if (!ruleMap.containsKey(key) || rule == null) {
            throw new IllegalStateException("Unknown source-key in derived keywords: " + 
                    type + "/" + key);
        }
        if (keywords.containsKey(key)) {
            return keywords.get(key).values;
        }
        if (keysWaitingInStack.contains(key)) {
            throw new IllegalStateException("Circular dependency in derived keywords: " +
                    type + " / " + keysWaitingInStack);
        }
        if (!rule.isDerivedKey()) {
            throw new IllegalStateException("Reference to not derived keyword with no value: " +
                    type + "/" + key);
        }
        keysWaitingInStack.add(key);
        List<Object> values = processDerivedRule(type, ruleMap, rule.getSourceKey().get(),
                keywords, lookup, keysWaitingInStack, objectRefPath);
        if (rule.getTransform().isPresent() &&
                rule.getTransform().get().getSubobjectIdKey().isPresent()) {
            processDerivedRule(type, ruleMap, rule.getTransform().get().getSubobjectIdKey().get(),
                    keywords, lookup, keysWaitingInStack, objectRefPath);
        }
        for (Object value : values) {
            processRule(type, rule, key, value, keywords, lookup, objectRefPath);
        }
        keysWaitingInStack.remove(key);
        List<Object> ret = keywords.containsKey(key) ? keywords.get(key).values : new ArrayList<>();
        if (isEmpty(ret) && rule.getDefaultValue().isPresent()) {
            addOrAddAll(rule.getDefaultValue().get(), ret);
        }
        return ret;
    }

    private static boolean isEmpty(Object value) {
        return value == null || (value instanceof List && ((List<?>)value).isEmpty());
    }
    
    private static void processRule(String type, IndexingRules rule, String key, Object value,
            Map<String, InnerKeyValue> keywords, ObjectLookupProvider lookup,
            List<GUID> objectRefPath)
            throws IndexingException, InterruptedException, ObjectParseException {
        Object valueFinal = value;
        if (valueFinal == null) {
            if (rule.getDefaultValue().isPresent()) {
                valueFinal = rule.getDefaultValue().get();
            } else {
                valueFinal = Collections.EMPTY_LIST;
            }
        }
        InnerKeyValue values = keywords.get(key);
        if (values == null) {
            values = new InnerKeyValue();
            values.values = new ArrayList<>();
            keywords.put(key, values);
        }
        values.notIndexed = rule.isNotIndexed();
        if (rule.getTransform().isPresent()) {
            valueFinal = transform(valueFinal, rule, keywords, lookup, objectRefPath);
        }
        addOrAddAll(valueFinal, values.values);
    }
    
    @SuppressWarnings("unchecked")
    private static void addOrAddAll(Object valueFinal, List<Object> values) {
        if (valueFinal != null) {
            if (valueFinal instanceof List) {
                values.addAll((List<Object>)valueFinal);
            } else {
                values.add(valueFinal);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Object transform(Object value, IndexingRules rule,
            Map<String, InnerKeyValue> sourceKeywords, ObjectLookupProvider lookup,
            List<GUID> objectRefPath)
            throws IndexingException, InterruptedException, ObjectParseException {
        final Transform transform = rule.getTransform().get();
        switch (transform.getType()) {
        case location:
            List<List<Object>> loc = (List<List<Object>>)value;
            Map<LocationTransformType, Object> retLoc = new LinkedHashMap<>();
            //TODO CODE if the subobject stuff in the ObjectParsingRules is left out this throws an indexing exception. Need to figure out cause.
            retLoc.put(LocationTransformType.contig_id, loc.get(0).get(0));
            String strand = (String)loc.get(0).get(2);
            retLoc.put(LocationTransformType.strand, strand);
            int start = (Integer)loc.get(0).get(1);
            int len = (Integer)loc.get(0).get(3);
            retLoc.put(LocationTransformType.length, len);
            retLoc.put(LocationTransformType.start,
                    strand.equals("+") ? start : (start - len + 1));
            retLoc.put(LocationTransformType.stop, strand.equals("+") ? (start + len - 1) : start);
            if (!transform.getLocation().isPresent()) {
                return retLoc;
            }
            return retLoc.get(transform.getLocation().get());
        case values:
            if (value == null) {
                return null;
            }
            if (value instanceof List) {
                List<Object> input = (List<Object>)value;
                List<Object> ret = new ArrayList<>();
                for (Object item : input) {
                    addOrAddAll(transform(item, rule, sourceKeywords, lookup, objectRefPath), ret);
                }
                return ret;
            }
            if (value instanceof Map) {
                Map<String, Object> input = (Map<String, Object>)value;
                List<Object> ret = new ArrayList<>();
                for (Object item : input.values()) {
                    addOrAddAll(transform(item, rule, sourceKeywords, lookup, objectRefPath), ret);
                }
                return ret;
            }
            return String.valueOf(value);
        case string:
            return String.valueOf(value);
        case integer:
            return Integer.parseInt(String.valueOf(value));
        case guid:
            String type = transform.getTargetObjectType().get();
            if (type == null) {
                throw new IllegalStateException("Target object type should be set for 'guid' " +
                        "transform");
            }
            final ObjectTypeParsingRules typeDescr = lookup.getTypeDescriptor(type);
            final String storageCode = typeDescr.getStorageObjectType().getStorageCode();
            final Set<String> refs = toStringSet(value);
            final Set<GUID> unresolvedGUIDs;
            try {
                unresolvedGUIDs = refs.stream().map(r -> GUID.fromRef(storageCode, r))
                        .collect(Collectors.toSet());
            } catch (IllegalArgumentException e) {
                throw new ObjectParseException(e.getMessage(), e);
            }
            Set<GUID> guids = lookup.resolveRefs(objectRefPath, unresolvedGUIDs);
            Set<String> subIds = null;
            if (transform.getSubobjectIdKey().isPresent()) {
                if (!typeDescr.getSubObjectType().isPresent()) {
                    //TODO CODE check this in parsing rules creation context if possible
                    throw new IllegalStateException("Subobject GUID transform should correspond " +
                            "to subobject type descriptor: " + rule);
                }
                subIds = toStringSet(
                        sourceKeywords.get(transform.getSubobjectIdKey().get()).values);
                if (guids.size() != 1) {
                    throw new IllegalStateException("In subobject IDs case source keyword " + 
                            "should point to value with only one parent object reference");
                }
                GUID parentGuid = guids.iterator().next();
                guids = new LinkedHashSet<>();
                for (String subId : subIds) {
                    guids.add(new GUID(typeDescr.getStorageObjectType().getStorageCode(),
                            parentGuid.getAccessGroupId(),
                            parentGuid.getAccessGroupObjectId(), parentGuid.getVersion(), 
                            typeDescr.getSubObjectType().get(), subId));
                }
            }
            Map<GUID, String> guidToType = lookup.getTypesForGuids(guids);
            for (GUID guid : guids) {
                if (!guidToType.containsKey(guid)) {
                    throw new IllegalStateException("GUID " + guid + " not found");
                }
                String actualType = guidToType.get(guid);
                if (!actualType.equals(type)) {
                    throw new IllegalStateException("GUID " + guid + " has unexpected type: " + 
                            actualType);
                }
            }
            return guids.stream().map(GUID::toString).collect(Collectors.toList());
        case lookup:
            /* TODO CODE or DOCUMENTATION it appears that this only works if sourceKey = true and the sourceKey is a GUID transform. Check and document. */
            final String retProp = transform.getTargetKey().get();
            Set<String> guidText = toStringSet(value);
            Map<GUID, ObjectData> guidToObj = lookup.lookupObjectsByGuid(
                    guidText.stream().map(GUID::new).collect(Collectors.toSet()));
            List<Object> ret = new ArrayList<>();
            for (ObjectData obj : guidToObj.values()) {
                if (retProp.startsWith("key.")) {
                    String key = retProp.substring(4);
                    ret.add(obj.keyProps.get(key));
                } else if (retProp.equals("oname")) {
                    ret.add(obj.objectName);
                }
            }
            return ret;
        default:
            // java whines unless this is here, but transform is guaranteed to be one of the above
            throw new RuntimeException("someone did something naughty");
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> toStringSet(Object value) {
        Set<String> refs = new LinkedHashSet<>();
        if (value instanceof List) {
            for (Object obj : (List<Object>)value) {
                refs.add(String.valueOf(obj));
            }
        } else {
            refs.add(String.valueOf(value));
        }
        return refs;
    }
    
    private static void extractIndexingPart(String json, boolean fromParent,
            List<IndexingRules> indexingRules, ValueConsumer<List<IndexingRules>> consumer)
            throws IOException, ObjectParseException, JsonParseException,
            IndexingException, InterruptedException {
        Map<ObjectJsonPath, List<IndexingRules>> pathToRules = new LinkedHashMap<>();
        for (IndexingRules rules : indexingRules) {
            if (rules.isDerivedKey()) {
                continue;
            }
            if (!rules.getPath().isPresent()) {
                throw new IllegalStateException("Path should be defined for non-derived " +
                        "indexing rules");
            }
            if (rules.isFromParent() != fromParent) {
                continue;
            }
            List<IndexingRules> rulesList = pathToRules.get(rules.getPath().get());
            if (rulesList == null) {
                rulesList = new ArrayList<>();
                pathToRules.put(rules.getPath().get(), rulesList);
            }
            rulesList.add(rules);
        }
        ValueCollectingNode<List<IndexingRules>> root = new ValueCollectingNode<>();
        for (ObjectJsonPath path : pathToRules.keySet()) {
            root.addPath(path, pathToRules.get(path));
        }
        ValueCollector<List<IndexingRules>> collector = new ValueCollector<List<IndexingRules>>();
        try (JsonParser jp = UObject.getMapper().getFactory().createParser(json)) {
            collector.mapKeys(root, jp, consumer);
        }
    }

    public interface ObjectLookupProvider {
        public Set<GUID> resolveRefs(List<GUID> objectRefPath, Set<GUID> unresolvedGUIDs) 
                throws IndexingException, InterruptedException;
        public Map<GUID, String> getTypesForGuids(Set<GUID> guids)
                throws InterruptedException, IndexingException;
        public Map<GUID, ObjectData> lookupObjectsByGuid(Set<GUID> guids) 
                throws InterruptedException, IndexingException;
        public ObjectTypeParsingRules getTypeDescriptor(String type) throws IndexingException;
    }

    private static class InnerKeyValue {
        boolean notIndexed;
        List<Object> values;
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("InnerKeyValue [notIndexed=");
            builder.append(notIndexed);
            builder.append(", values=");
            builder.append(values);
            builder.append("]");
            return builder.toString();
        }
    }
}
