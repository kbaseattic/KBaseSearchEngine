package kbasesearchengine.authorization;

import java.util.Map;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.io.IOException;

public class AuthAPI {

    private final String authURLString;

    /** Wrapper for Auth API. Currently has a method to get display names of users.
     * @param authURL URL string for the auth server
     */
    public AuthAPI(final String authURLString) {
        this.authURLString = authURLString;
    }

    public final Map<String, Object> getDisplayNames(
            final String token,
            final String userList)
            throws Exception {

        // TODO   needs to be updated to use the new API
        String urlString = authURLString + "/testmode/api/V2/testmodeonly/user/" + userList;
        //String urlString = authURLString + "/testmode/api/V2/users/?list=" + userList;

        final URL url = new URL(urlString);
        final HttpURLConnection con = (HttpURLConnection) url.openConnection();

        con.setRequestMethod("GET");
        con.setRequestProperty("authorization", token);

        final int responseCode = con.getResponseCode();

        //System.out.println("Response Code : " + responseCode);

        if (responseCode == 500) {
            return new HashMap<String, Object>() {{ put("display", null); }};
        }
        if (responseCode != 200) {
            final String err = IOUtils.toString(con.getErrorStream());
            System.out.println(err);
            throw new IOException(err.substring(1, 200));
        }

        final HashMap<String, Object> result;
        result = (HashMap<String, Object>) new ObjectMapper().readValue(con.getInputStream(), HashMap.class);

        return result;
    }
}
