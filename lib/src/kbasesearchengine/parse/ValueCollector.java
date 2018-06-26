package kbasesearchengine.parse;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import kbasesearchengine.common.JsonTokenUtil;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import us.kbase.common.service.UObject;

/**
 * Extraction of key values based on JSON token stream.
 * @author rsutormin
 */
public class ValueCollector <T> {

    public void mapKeys(ValueCollectingNode<T> tree, JsonParser jts, 
            ValueConsumer<T> consumer)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {
        JsonToken t = jts.nextToken();
        mapKeysWithOpenToken(jts, t, tree, consumer, new ArrayList<String>());
    }
    
    /*
     * This is main recursive method for tracking current token place in subset schema tree
     * and making decisions whether or not we need to process this token or block of tokens or
     * just skip it.
     */
    private void mapKeysWithOpenToken(
            final JsonParser jts,
            final JsonToken current, 
            final ValueCollectingNode<T> selection,
            final ValueConsumer<T> consumer,
            final List<String> path)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {
        JsonToken t = current;
        if (selection.getRules() != null && (t == JsonToken.START_OBJECT || t == JsonToken.START_ARRAY)) {
            StringWriter outChars = new StringWriter();
            JsonGenerator jsonGen = UObject.getMapper().getFactory().createGenerator(outChars);
            JsonTokenUtil.writeTokensFromCurrent(jts, current, jsonGen);
            jsonGen.close();
            Object value = UObject.transformStringToObject(outChars.toString(), Object.class);
            consumer.addValue(selection.getRules(), value);
            return;
        }
		if (t == JsonToken.START_OBJECT) {	// we observe open of mapping/object in real json data
		    if (selection.getRules() != null) {
                throw new ObjectParseException("Invalid ID mapping selection: object cannot be " +
                        "collected as value, at: " + ObjectJsonPath.getPathText(path));
		    }
			if (selection.hasChildren()) {	// we have some restrictions for this object in selection
				// we will remove visited keys from selectedFields and check emptiness at object end
				Set<String> selectedFields = new LinkedHashSet<String>(
				        selection.getChildren().keySet());
				boolean all = false;
				ValueCollectingNode<T> allChild = null;
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
				while (true) {
					t = jts.nextToken();
					if (t == JsonToken.END_OBJECT) {
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
						// read first token of value block in order to prepare state for recursive 
						// extractFieldsWithOpenToken call
						t = jts.nextToken();
						// add field to the tail of path branch
						path.add(fieldName);
						// process value corresponding to this field recursively
						mapKeysWithOpenToken(jts, t, all ? allChild : 
							selection.getChildren().get(fieldName), consumer, path);
						// remove field from tail of path branch
						path.remove(path.size() - 1);
					} else {
						// otherwise we skip value following after field
						t = jts.nextToken();
						JsonTokenUtil.skipChildren(jts, t);
					}
				}
			} else {
			    JsonTokenUtil.skipChildren(jts, t);
			}
		} else if (t == JsonToken.START_ARRAY) {
		    // we observe open of array/list in real json data
			if (selection.hasChildren()) {
			    // we have some restrictions for array item positions in selection
				Set<String> selectedFields = new LinkedHashSet<String>(
				        selection.getChildren().keySet());
				ValueCollectingNode<T> allChild = null;
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
				for (int pos = 0; ; pos++) {
					t = jts.nextToken();
					if (t == JsonToken.END_ARRAY) {
						break;
					}
					ValueCollectingNode<T> child = null;
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
						mapKeysWithOpenToken(jts, t, child, consumer, path);
						// remove field from tail of path branch
						path.remove(path.size() - 1);
					}
				}
			} else {
			    JsonTokenUtil.skipChildren(jts, t);
			}
		} else {	
		    // we observe scalar value (text, integer, double, boolean, null) in real json data
			if (selection.hasChildren()) {
			    if (selection.getChildren().size() != 1 || 
			            !selection.getChildren().containsKey("{size}"))
				throw new ObjectParseException("Invalid selection: the path given specifies " +
						"fields or elements that do not exist because data at this location is " +
						"a scalar value (i.e. string, integer, float), at: " + 
						ObjectJsonPath.getPathText(path));
			}
			T rules = selection.getRules();
			if (rules == null && selection.getChildren().size() == 1 && 
			        selection.getChildren().containsKey("{size}")) {
			    rules = selection.getChildren().get("{size}").getRules();
			}
			if (rules != null) {
			    Object value = JsonTokenUtil.getCurrentTokenPrimitive(jts, t);
			    consumer.addValue(rules, value);
			}
		}
	}
}
