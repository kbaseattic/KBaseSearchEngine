package kbasesearchengine.test.main;

import static kbasesearchengine.test.events.handler.WorkspaceEventHandlerTest.wsTuple;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.ObjectData;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.main.NarrativeInfoDecorator;
import kbasesearchengine.main.SearchInterface;
import us.kbase.common.service.Tuple5;

public class NarrativeInfoDecoratorTest {
    
    @Test
    public void searchObjectsDecorateWithNoData() throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final SearchInterface search = mock(SearchInterface.class);
        
        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, weh);
        
        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */
        
        final SearchObjectsInput dummyInput = new SearchObjectsInput();
        
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(Collections.emptyList())
                .withAccessGroupNarrativeInfo(null));
        
        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");
        
        assertThat("incorrect object data", res.getObjects(), is(Collections.emptyList()));
        assertThat("incorrect narr info", res.getAccessGroupNarrativeInfo(),
                is(Collections.emptyMap()));
    }
    
    @Test
    public void searchObjectsDecorateWithNullInfo() throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final SearchInterface search = mock(SearchInterface.class);
        
        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, weh);
        
        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */
        
        final SearchObjectsInput dummyInput = new SearchObjectsInput();
        
        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"),
                new ObjectData().withGuid("WS:42/7/21"),
                new ObjectData().withGuid("WS:1/61/1"),
                new ObjectData().withGuid("FS:6/22/3"), // expect skip
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(null));
        
        // no narrative info at all
        when(weh.getWorkspaceInfo(65)).thenReturn(wsTuple(
                65, "name1", "owner1", "2018-02-08T21:55:45Z", 0, "r", "n", "unlocked",
                Collections.emptyMap()));
        
        // only narrative id
        when(weh.getWorkspaceInfo(1)).thenReturn(wsTuple(
                1, "name2", "owner2", "2018-02-08T21:55:57Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "2")));
        
        // only narrative name
        when(weh.getWorkspaceInfo(2)).thenReturn(wsTuple(
                2, "name3", "owner3", "2018-02-08T21:55:45.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative_nice_name", "myhorridnarrative")));
        
        // full narrative info
        when(weh.getWorkspaceInfo(42)).thenReturn(wsTuple(
                42, "name4", "owner4", "2018-02-08T21:55:50.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative", "3", "narrative_nice_name", "mylovelynarrative")));
        
        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");
        
        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = new HashMap<>();
        expected.put(65L, narrInfo(null, null, 1518126945000L, "owner1", null));
        expected.put(1L, narrInfo(null, null, 1518126957000L, "owner2", null));
        expected.put(2L, narrInfo(null, null, 1518126945678L, "owner3", null));
        expected.put(42L, narrInfo("mylovelynarrative", 3L, 1518126950678L, "owner4", null));
        
        assertThat("incorrect object data", res.getObjects(), is(objectdata));
        
        compare(res.getAccessGroupNarrativeInfo(), expected);
    }
    
    @Test
    public void searchObjectsDecorateWithPreexistingInfo() throws Exception {
        final WorkspaceEventHandler weh = mock(WorkspaceEventHandler.class);
        final SearchInterface search = mock(SearchInterface.class);
        
        final NarrativeInfoDecorator nid = new NarrativeInfoDecorator(search, weh);
        
        /* since a) the generated input class has no hashcode or equals and
         * b) the argument is just a straight pass through, we just use an identity match
         * for mockito to recognize the argument
         */
        
        final SearchObjectsInput dummyInput = new SearchObjectsInput();
        
        final List<ObjectData> objectdata = Arrays.asList(
                new ObjectData().withGuid("WS:65/2/7"), // expect overwrite
                new ObjectData().withGuid("WS:2/345/1000"));
        when(search.searchObjects(dummyInput, "user")).thenReturn(new SearchObjectsOutput()
                .withObjects(objectdata)
                .withAccessGroupNarrativeInfo(ImmutableMap.of(
                        65L, narrInfo("narrname", 1L, 10000L, "owner", "Herbert J. Kornfeld"),
                        32L, narrInfo("narrname6", 2L, 20000L, "owner6", "Herbert K. Kornfeld"))));
        
        // no narrative info at all
        when(weh.getWorkspaceInfo(65)).thenReturn(wsTuple(
                65, "name1", "owner1", "2018-02-08T21:55:45Z", 0, "r", "n", "unlocked",
                Collections.emptyMap()));
        
        // only narrative name
        when(weh.getWorkspaceInfo(2)).thenReturn(wsTuple(
                2, "name3", "owner3", "2018-02-08T21:55:45.678Z", 0, "r", "n", "unlocked",
                ImmutableMap.of("narrative_nice_name", "myhorridnarrative")));
        
        final SearchObjectsOutput res = nid.searchObjects(dummyInput, "user");
        
        final Map<Long, Tuple5<String, Long, Long, String, String>> expected = new HashMap<>();
        expected.put(65L, narrInfo(null, null, 1518126945000L, "owner1", null));
        expected.put(2L, narrInfo(null, null, 1518126945678L, "owner3", null));
        expected.put(32L, narrInfo("narrname6", 2L, 20000L, "owner6", "Herbert K. Kornfeld"));
        
        assertThat("incorrect object data", res.getObjects(), is(objectdata));
        
        compare(res.getAccessGroupNarrativeInfo(), expected);
    }
    
    private void compare(
            final Map<Long, Tuple5<String, Long, Long, String, String>> got,
            final Map<Long, Tuple5<String, Long, Long, String, String>> expected) {
        assertThat("incorrect map keys", got.keySet(), is(expected.keySet()));
        for (final Long key: got.keySet()) {
            compare(got.get(key), expected.get(key));
        }
    }

    private void compare(
            final Tuple5<String, Long, Long, String, String> got,
            final Tuple5<String, Long, Long, String, String> expected) {
        assertThat("incorrect narrative name", got.getE1(), is(expected.getE1()));
        assertThat("incorrect narrative id", got.getE2(), is(expected.getE2()));
        assertThat("incorrect epoch", got.getE3(), is(expected.getE3()));
        assertThat("incorrect owner", got.getE4(), is(expected.getE4()));
        assertThat("incorrect display name", got.getE5(), is(expected.getE5()));
    }

    private Tuple5<String, Long, Long, String, String> narrInfo(
            final String narrativeName,
            final Long narrativeId,
            final Long epoch,
            final String owner,
            final String displayName) {
        return new Tuple5<String, Long, Long, String, String>()
                .withE1(narrativeName)
                .withE2(narrativeId)
                .withE3(epoch)
                .withE4(owner)
                .withE5(displayName);
    }
    
}