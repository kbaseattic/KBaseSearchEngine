package kbasesearchengine.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import kbasesearchengine.common.JsonTokenUtil;
import kbasesearchengine.common.ObjectJsonPath;

/**
 * Extraction of searchable subset based on JSON token stream.
 * @author rsutormin
 */
public class SubObjectExtractor {

    /**
     * Extract the fields listed in selection from the element and add them to the subset.
     *
     * selection must either be an object containing structure field names to extract, '*' in the case of
     * extracting a mapping, or '[*]' for extracting a list.  if the selection is empty, nothing is added.
     * If extractKeysOf is set, and the element is an Object (ie a kidl mapping), then an array of the keys
     * is added instead of the entire mapping.
     *
     * we assume here that selection has already been validated against the structure of the document, so that
     * if we get true on extractKeysOf, it really is a mapping, and if we get a '*' or '[*]', it really is
     * a mapping or array.
     * @throws ObjectParseException
     */
    public static void extract(
            final ObjectJsonPath pathToSub,
            final List<ObjectJsonPath> objpaths,
            final JsonParser jts,
            final SubObjectConsumer consumer)
            throws IOException, ObjectParseException {
        //if the selection is empty, we return without adding anything
        SubObjectExtractionNode root = new SubObjectExtractionNode();
        SubObjectExtractionNode sub = root.addPath(pathToSub, true, false);
        for (ObjectJsonPath path: objpaths) {
            sub.addPath(JsonTokenUtil.trimPath(path), false, true);
        }
        extract(root, jts, consumer);
    }
    
    public static void extract(
            final SubObjectExtractionNode tree,
            final JsonParser jts,
            final SubObjectConsumer consumer)
            throws IOException, ObjectParseException {
        JsonToken t = jts.nextToken();
        extractFieldsWithOpenToken(jts, t, tree, consumer, new ArrayList<String>(),
                false, false, true);
        consumer.flush();
    }
    
    /*
     * This is main recursive method for tracking current token place in subset schema tree
     * and making decisions whether or not we need to process this token or block of tokens or
     * just skip it.
     */
    private static void extractFieldsWithOpenToken(
            final JsonParser jts,
            final JsonToken current,
            final SubObjectExtractionNode selection,
            final SubObjectConsumer consumer,
            final List<String> path,
            final boolean strictMaps,
            final boolean strictArrays,
            final boolean fromSkippedLevel)
            throws IOException, ObjectParseException {
        if (fromSkippedLevel && !selection.isSkipLevel()) {
            // It means we're starting sub-object (or whole object is needed)
            consumer.nextObject(ObjectJsonPath.getPathText(path));
        }
        JsonToken t = current;
        boolean skipLvl = selection.isSkipLevel();
        if (t == JsonToken.START_OBJECT) {    // we observe open of mapping/object in real json data
            if (selection.hasChildren()) {    // we have some restrictions for this object in selection
                // we will remove visited keys from selectedFields and check emptiness at object end
                Set<String> selectedFields = new LinkedHashSet<String>(
                        selection.getChildren().keySet());
                if (selectedFields.size() == 1 && selectedFields.contains("{size}")) {
                    int size = 0;
                    while (true) {
                        t = jts.nextToken();
                        if (t == JsonToken.END_OBJECT) {
                            break;
                        }
                        if (t != JsonToken.FIELD_NAME)
                            throw new ObjectParseException("Error parsing json format " + 
                                    t.asString() + ", at: " + ObjectJsonPath.getPathText(path));
                        t = jts.nextToken();
                        JsonTokenUtil.skipChildren(jts, t);
                        size++;
                    }
                    consumer.getOutput().writeNumber(size);
                } else {
                    boolean all = false;
                    SubObjectExtractionNode allChild = null;
                    if (selectedFields.contains("*")) {
                        all = true;
                        selectedFields.remove("*");
                        allChild = selection.getChildren().get("*");
                        if (selectedFields.size() > 0)
                            throw new ObjectParseException("Invalid selection: the selection path " +
                                    "contains both '*' to select all fields and selection of " +
                                    "specific fields (" + selectedFields + "), at: " + 
                                    ObjectJsonPath.getPathText(path));
                    }
                    // process first token standing for start of object
                    if (!skipLvl) {
                        JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());
                    }
                    while (true) {
                        t = jts.nextToken();
                        if (t == JsonToken.END_OBJECT) {
                            if (!skipLvl) {
                                JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());
                            }
                            break;
                        }
                        if (t != JsonToken.FIELD_NAME)
                            throw new ObjectParseException("Error parsing json format " + 
                                    t.asString() + ", at: " + ObjectJsonPath.getPathText(path));
                        String fieldName = jts.getText();
                        if (all || selectedFields.contains(fieldName)) {
                            // if we need all fields or the field is present in list of necessary fields 
                            // we process it and value following after that
                            if (!all)
                                selectedFields.remove(fieldName);
                            if (!skipLvl) {
                                JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());
                            }
                            // read first token of value block in order to prepare state for recursive 
                            // extractFieldsWithOpenToken call
                            t = jts.nextToken();
                            // add field to the tail of path branch
                            path.add(fieldName);
                            // process value corresponding to this field recursively
                            extractFieldsWithOpenToken(jts, t, all ? allChild : 
                                selection.getChildren().get(fieldName), consumer, path, 
                                strictMaps, strictArrays, selection.isSkipLevel());
                            // remove field from tail of path branch
                            path.remove(path.size() - 1);
                        } else {
                            // otherwise we skip value following after field
                            t = jts.nextToken();
                            JsonTokenUtil.skipChildren(jts, t);
                        }
                    }
                    // let's check have we visited all selected fields in this map
                    // we will not visit them in real data and hence will not delete them from selection
                    if (strictMaps && !selectedFields.isEmpty()) {
                        String notFound = selectedFields.iterator().next();
                        throw new ObjectParseException("Invalid selection: data does not contain " +
                                "a field or key named '" + notFound + "', at: " + 
                                getPathText(path, notFound));
                    }
                }
            } else {  // need all fields and values
                if (selection.isNeedAll()) {
                    JsonTokenUtil.writeTokensFromCurrent(jts, t, consumer.getOutput());
                } else {
                    JsonTokenUtil.skipChildren(jts, t);
                }
            }
        } else if (t == JsonToken.START_ARRAY) {    // we observe open of array/list in real json data
            if (selection.hasChildren()) {  // we have some restrictions for array item positions in selection
                Set<String> selectedFields = new LinkedHashSet<String>(
                        selection.getChildren().keySet());
                if (selectedFields.size() == 1 && selectedFields.contains("{size}")) {
                    int size = 0;
                    while (true) {
                        t = jts.nextToken();
                        if (t == JsonToken.END_ARRAY) {
                            break;
                        }
                        JsonTokenUtil.skipChildren(jts, t);
                        size++;
                    }
                    consumer.getOutput().writeNumber(size);
                } else {
                    SubObjectExtractionNode allChild = null;
                    // now we support only '[*]' which means all elements and set of numbers in case of 
                    // certain item positions are selected in array
                    if (!selectedFields.contains("[*]")) {
                        for (String item : selectedFields) {
                            try {
                                Integer.parseInt(item);
                            } catch (NumberFormatException ex) {
                                throw new ObjectParseException("Invalid selection: data at '" +
                                        ObjectJsonPath.getPathText(path) + "' is an array, so " +
                                        "element selection must be an integer. You requested element" +
                                        " '" + item + "', at: " + ObjectJsonPath.getPathText(path));
                            }
                        }
                    }
                    if (selectedFields.contains("[*]")) {
                        selectedFields.remove("[*]");
                        allChild = selection.getChildren().get("[*]");
                        // if there is [*] keyword selected there shouldn't be anything else in selection
                        if (selectedFields.size() > 0)
                            throw new ObjectParseException("Invalid selection: the selection path " +
                                    "contains both '[*]' to select all elements and selection of " +
                                    "specific elements (" + selectedFields + "), at: " +
                                    ObjectJsonPath.getPathText(path));
                    }
                    if (!skipLvl) {
                        JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());  // write start of array into output
                    }
                    for (int pos = 0; ; pos++) {
                        t = jts.nextToken();
                        if (t == JsonToken.END_ARRAY) {
                            if (!skipLvl) {
                                JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());
                            }
                            break;
                        }
                        SubObjectExtractionNode child = null;
                        if (allChild != null) {
                            child = allChild;
                        } else {
                            String key = "" + pos;
                            if (selection.getChildren().containsKey(key)) {
                                child = selection.getChildren().get(key);
                                selectedFields.remove(key);
                            }
                        }
                        if (child == null) {
                            // this element of array is not selected, skip it
                            JsonTokenUtil.skipChildren(jts, t);
                        } else {
                            // add element position to the tail of path branch
                            path.add("" + pos);
                            // process value of this element recursively
                            extractFieldsWithOpenToken(jts, t, child, consumer, path, strictMaps,
                                    strictArrays, selection.isSkipLevel());
                            // remove field from tail of path branch
                            path.remove(path.size() - 1);
                        }
                    }
                    // let's check have we visited all selected items in this array
                    if (strictArrays && !selectedFields.isEmpty()) {
                        String notFound = selectedFields.iterator().next();
                        throw new ObjectParseException("Invalid selection: no array element exists " +
                                "at position '" + notFound + "', at: " + getPathText(path, notFound));
                    }
                }
            } else {
                if (selection.isNeedAll()) {
                    // need all elements
                    JsonTokenUtil.writeTokensFromCurrent(jts, t, consumer.getOutput());
                } else {
                    JsonTokenUtil.skipChildren(jts, t);
                }
            }
        } else {    // we observe scalar value (text, integer, double, boolean, null) in real json data
            if (selection.hasChildren())
                throw new ObjectParseException("Invalid selection: the path given specifies " +
                        "fields or elements that do not exist because data at this location is " +
                        "a scalar value (i.e. string, integer, float), at: " +
                        ObjectJsonPath.getPathText(path));
            JsonTokenUtil.writeCurrentToken(jts, t, consumer.getOutput());
        }
    }

    public static String getPathText(List<String> path, String add) {
        path.add(add);
        String ret = ObjectJsonPath.getPathText(path);
        path.remove(path.size() - 1);
        return ret;
    }
    
}
