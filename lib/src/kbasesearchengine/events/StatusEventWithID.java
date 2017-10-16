package kbasesearchengine.events;

import kbasesearchengine.tools.Utils;

public class StatusEventWithID {
    
    //TODO JAVADOC
    //TODO TEST
    
    private final StatusEvent event;
    private final StatusEventID id;
    
    public StatusEventWithID(final StatusEvent event, final StatusEventID id) {
        Utils.nonNull(event, "event");
        Utils.nonNull(id, "id");
        this.event = event;
        this.id = id;
    }

    public StatusEvent getEvent() {
        return event;
    }

    public StatusEventID getId() {
        return id;
    }

}
