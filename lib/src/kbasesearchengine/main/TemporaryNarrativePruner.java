package kbasesearchengine.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.tools.Utils;

/** This class is a Q&D fix to reduce the transport problems when returning narratives from
 * search, since they contain a number of fields which are extremely long strings often full of
 * javascript that are useless for displaying to a user.
 * 
 * Longer term fix will be adding the ability to run custom code for a type when indexing said
 * type.
 * 
 * Currently the class is only tested via integration tests as it is expected to have a 
 * relatively short lifetime.
 * 
 * @author gaprice@lbl.gov
 *
 */
public class TemporaryNarrativePruner implements SearchInterface {

    // narrative fields to remove.
    private static final List<String> FIELDS_TO_NUKE = Arrays.asList(
            "source", "code_output", "app_output", "app_info", "app_input", "job_ids");
    
    private final SearchInterface source;
    
    /** Create a narrative pruner.
     * @param source the wrapped {@link SearchInterface}.
     */
    public TemporaryNarrativePruner(final SearchInterface source) {
        Utils.nonNull(source, "source");
        this.source = source;
    }

    @Override
    public SearchTypesOutput searchTypes(final SearchTypesInput params, final String user)
            throws Exception {
        return source.searchTypes(params, user);
    }

    @Override
    public SearchObjectsOutput searchObjects(final SearchObjectsInput params, final String user)
            throws Exception {
        final SearchObjectsOutput searchObjects = source.searchObjects(params, user);
        return searchObjects.withObjects(clean(searchObjects.getObjects()));
    }

    @Override
    public GetObjectsOutput getObjects(GetObjectsInput params, String user) throws Exception {
        final GetObjectsOutput getObjects = source.getObjects(params, user);
        return getObjects.withObjects(clean(getObjects.getObjects()));
    }

    @Override
    public Map<String, TypeDescriptor> listTypes(final String uniqueType) throws Exception {
        return source.listTypes(uniqueType);
    }

    // this method modifies the input in place
    private List<ObjectData> clean(final List<ObjectData> objectData) {
        for (final ObjectData od: objectData) {
            if (od.getType().equals("Narrative")) {
                od.setData(null); // nuke the raw data
                final Map<String, String> keyprops = new HashMap<>(od.getKeyProps());
                for (final String field: FIELDS_TO_NUKE) {
                    keyprops.remove(field);
                }
                od.setKeyProps(keyprops);
            }
        }
        return objectData;
    }
    
}
