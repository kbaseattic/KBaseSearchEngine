package kbasesearchengine.system;

import java.util.Comparator;


/** Orders {@link ObjectTypeParsingRules} such that parsing rules for subobjects are first in the
 * order.
 * @author gaprice@lbl.gov
 *
 */
public class ParsingRulesSubtypeFirstComparator implements Comparator<ObjectTypeParsingRules> {

    @Override
    public int compare(final ObjectTypeParsingRules o1, final ObjectTypeParsingRules o2) {
        if (o1.getSubObjectType().isPresent()) {
            if (o2.getSubObjectType().isPresent()) {
                return 0;
            } else {
                return -1;
            }
        } else if (o2.getSubObjectType().isPresent()) {
            return 1;
        } else {
            return 0;
        }
    }

}
