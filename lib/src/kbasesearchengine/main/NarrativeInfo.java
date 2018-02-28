package kbasesearchengine.main;

import kbasesearchengine.tools.Utils;

/** Holds information from workspace related to the narrative.
 * Used by AuthCache to save narrative information against workspace ID.
 * @author ganapathy@bnl.gov
 *
 */
public class NarrativeInfo {

    private static final int NUM_ELEMENTS = 4; // TODO: If this is not required for cache, this can be removed.
    private String narrativeName;
    private Long narrativeId;
    private Long timeLastSaved;
    private String wsOwnerUsername;

    /** Constructs the narrative info got from workspace. Used by tests.
     *  @param narrativeName the name of the narrative contained in the workspace,
     *                       or null if the workspace does not contain a narrative.
     *  @param narrativeId the id of the narrative contained in the workspace, or null.
     *  @param timeLastSaved the modification time of the workspace in epoch milli.
     *  @param wsOwnerUsername the unique user name of the workspace's owner.
     */
    public NarrativeInfo(final String narrativeName,
                         final Long narrativeId,
                         final Long timeLastSaved,
                         final String wsOwnerUsername){
        this.narrativeName = narrativeName;
        this.narrativeId = narrativeId;
        Utils.nonNull(timeLastSaved, "Time Last Saved");
        this.timeLastSaved = timeLastSaved;
        Utils.nonNull(wsOwnerUsername, "Workspace Owner Username");
        this.wsOwnerUsername = wsOwnerUsername;
    }

    /** Sets the narrative name
     * @param narrativeName the name of the narrative contained in the workspace,
     * or null if the workspace does not contain a narrative.
     */
    public NarrativeInfo withNarrativeName(final String narrativeName) {
        this.narrativeName = narrativeName;
        return this;
    }

    /** Sets the narrative ID
     * @param narrativeId the id of the narrative contained in the workspace, or null.
     */
    public NarrativeInfo withNarrativeId(final Long narrativeId) {
        this.narrativeId = narrativeId;
        return this;
    }

    /** Sets the modification time of the workspace.
     * @param timeLastSaved the modification time of the workspace in epoch milli.
     */
    public NarrativeInfo withTimeLastSaved(final Long timeLastSaved) {
        Utils.nonNull(timeLastSaved, "Time Last Saved");
        this.timeLastSaved = timeLastSaved;
        return this;
    }

    /** Sets the Workspace Owner Username
     * @param wsOwnerUsername the unique user name of the workspace's owner.
     */
    public NarrativeInfo withWsOwnerUsername(final String wsOwnerUsername) {
        Utils.nonNull(wsOwnerUsername, "Workspace Owner Username");
        this.wsOwnerUsername = wsOwnerUsername;
        return this;
    }

    /** Get the Narrative Name.
     * @return the narrativeName.
     */
    public String getNarrativeName() {
        return narrativeName;
    }

    /** Get the Narrative ID.
     * @return the commit.
     */
    public Long getNarrativeId() {
        return narrativeId;
    }

    /** Get the Time Last Saved.
     * @return the timeLastSaved.
     */
    public Long getTimeLastSaved() {
        return timeLastSaved;
    }

    /** Get the Workspace Owner Username.
     * @return the wsOwnerUsername.
     */
    public String getWsOwnerUsername() { return wsOwnerUsername; }

    /** Get the size of this object's contents.
     * @return the number of elements.
     */
    // TODO: If this is not required for cache, this can be removed.
    public int getNumElements() { return NUM_ELEMENTS; }
}
