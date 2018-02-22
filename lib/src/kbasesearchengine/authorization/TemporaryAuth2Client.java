package kbasesearchengine.authorization;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/** A temporary client for the KBase auth2 service. At some point this will be replaced with
 * a real client. Only supports methods that are needed right now.
 * @author gaprice@lbl.gov
 *
 */
public class TemporaryAuth2Client implements AuthInfoProvider {
    
    //TODO TEST this will need a mocked auth server to test some of the error handling, noted below. We leave this for later.
    
    private static final TypeReference<Map<String, Object>> MAP_STRING_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};

    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private final URL authURL;
    private String token;
    
    public TemporaryAuth2Client(URL authURL) {
        Utils.nonNull(authURL, "authURL");
        if (!authURL.toString().endsWith("/"))
            try {
                authURL = new URL(authURL.toString() + "/");
            } catch (MalformedURLException e) {
                throw new RuntimeException("this should never happen", e);
            }
        this.authURL = authURL;
    }

    /** Set token to be used in the auth API (to get display names)
     * will not be shown in the results.
     * @param token an auth token.
     */
    public TemporaryAuth2Client withToken(String token) {
        Utils.nonNull(token, "Token");
        this.token = token;
        return this;
    }

    /** Return the url of the auth service this client contacts.
     * @return the url.
     */
    public URL getURL() {
        return authURL;
    }
    
    /** Get display names for a set of users. Users that do not exist in the auth service
     * will not be shown in the results.
     * @param userNames the set of usernames to process.
     * @return a mapping of username to display name for each user.
     * @throws IOException if an IO error occurs.
     * @throws Auth2Exception if the auth service returns an exception.
     */
    public Map<String, String> getUserDisplayNames(
            final String token,
            final Set<String> userNames)
            throws IOException, Auth2Exception {
        Utils.notNullOrEmpty(token, "token cannot be null or whitespace only");
        Utils.nonNull(userNames, "userNames");
        for (final String name: userNames) {
            if (Utils.isNullOrEmpty(name)) {
                throw new IllegalArgumentException("Null or whitespace only entry in userNames");
            }
        }
        if (userNames.isEmpty()) {
            return Collections.emptyMap();
        }
        final String users = String.join(",", userNames);
        
        final URL target;
        try {
            target = new URL(authURL.toString() + "api/V2/users?list=" + users);
        } catch (MalformedURLException e) {
            throw new RuntimeException("this should never happen", e); 
        }
        final HttpURLConnection conn = (HttpURLConnection) target.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("accept", "application/json");
        conn.setRequestProperty("authorization", token);
        conn.setDoOutput(true);
        
        if (conn.getResponseCode() != 200) {
            try (final InputStream error = conn.getErrorStream()) {
                throw toException(error, conn.getResponseCode());
            }
        }
        // really we should translate to a string and catch json exceptions in case the url
        // points to a non-auth service. Getting a 200 in that case seems pretty unlikely though.
        try (final InputStream input = conn.getInputStream()) {
            return MAPPER.readValue(input, MAP_STRING_TYPE_REFERENCE);
        }
    }

    /** Get display names for a set of users. Users that do not exist in the auth service
     * will not be shown in the results.
     * @param userNames the set of usernames to process.
     * @return a mapping of username to display name for each user.
     * @throws IOException if an IO error occurs.
     * @throws Auth2Exception if the auth service returns an exception.
     */
    public Map<String, String> findUserDisplayNames(
            final Set<String> userNames)
            throws IOException, Auth2Exception {
        return this.getUserDisplayNames(token, userNames);
    }

    /** Get display name for a single user. User that does not exist in the auth service
     * will not be shown in the result.
     * @param userName the username to process.
     * @return the display name for the given username.
     * @throws IOException if an IO error occurs.
     * @throws Auth2Exception if the auth service returns an exception.
     */
    @Override
    public String findUserDisplayName(final String userName)
            throws IOException, Auth2Exception {
        final Set<String> userIds = new HashSet<>();
        userIds.add(userName);
        return this.findUserDisplayNames(userIds).getOrDefault(userName, null);
    }

    private Auth2Exception toException(final InputStream inputStream, final int responseCode)
            throws IOException {
        final String err = IOUtils.toString(inputStream);
        try {
            final Map<String, Object> errmap = MAPPER.readValue(err, MAP_STRING_TYPE_REFERENCE);
            return new Auth2Exception(errmap, responseCode);
        } catch (JsonProcessingException e) {
            // TODO need a mock server to test this case
            return new Auth2Exception(err, responseCode);
        }
    }
    
    /** An auth2 service exception.
     * @author gaprice@lbl.gov
     *
     */
    public static class Auth2Exception extends Exception {

        private static final long serialVersionUID = 1L;
        private final String callID;
        
        /** Create a new exception.
         * @param message the exception message.
         */
        public Auth2Exception(final String message) {
            super(message);
            callID = null;
        }
        
        // TODO need a mock server to test this case
        private Auth2Exception(final String error, final int responseCode) {
            super(String.format("Auth service responded with code %s: %s", responseCode,
                    truncateError(error)));
            callID = null;
        }
        
        private Auth2Exception(final Map<String, Object> authResponse, final int responseCode) {
            super(handleResponse(authResponse, responseCode));
            final ErrorContents ec = getErrorContents(authResponse);
            if (ec == null) {
                callID = null;
            } else {
                callID = ec.callid;
            }
            
        }
        
        private static String handleResponse(
                final Map<String, Object> authResponse,
                final int responseCode) {
            final ErrorContents ec = getErrorContents(authResponse);
            if (ec == null) {
                // TODO TEST need a mock server to test this case
                try {
                    return String.format(
                            "Auth service returned unexpected error structure with code %s: %s",
                            responseCode, MAPPER.writeValueAsString(authResponse));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("this should never happen", e);
                }
            }
            return String.format("Auth service returned error code %s with call id %s: %s",
                    responseCode, ec.callid, ec.message);
        }
        
        private static ErrorContents getErrorContents(final Map<String, Object> authResponse) {
            if (!authResponse.containsKey("error")) {
                // TODO TEST need a mock server to test this case
                return null;
            }
            // at this point we assume the error structure is ok
            @SuppressWarnings("unchecked")
            final Map<String, Object> error = (Map<String, Object>) authResponse.get("error");
            
            return new ErrorContents(error);
        }
        
        private static class ErrorContents {
            
            // the unused fields are provided for future use
            @SuppressWarnings("unused")
            public final int code;
            @SuppressWarnings("unused")
            public final String status;
            public final String message;
            public final String callid;
            @SuppressWarnings("unused")
            public final long timestamp;
            @SuppressWarnings("unused")
            public final Integer appcode;
            @SuppressWarnings("unused")
            public final String apperror;
            
            private ErrorContents(final Map<String, Object> error) {
                this.code = (int) error.get("httpcode");
                this.status = (String) error.get("httpstatus");
                this.message = (String) error.get("message");
                this.callid = (String) error.get("callid");
                this.timestamp = (long) error.get("time");
                // the next fields may or may not be present depending on the error e.g. not found
                // will not have app information. However, if they exist they will be embedded in
                // the message.
                this.appcode = (Integer) error.get("appcode");
                this.apperror = (String) error.get("apperror");
            }
        }

        private static String truncateError(String error) {
            // TODO need a mock server to test this case
            if (error.length() > 200) {
                error = error.substring(0, 197) + "...";
            }
            return error;
        }
        
        /** Get the call id returned with the exception, if available. The auth service
         * always returns a call id.
         * @return the call id.
         */
        public Optional<String> getCallID() {
            return Optional.of(callID);
        }
    }
}
