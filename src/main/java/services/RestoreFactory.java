package services;

import application.SQLiteProperties;
import auth.HqAuth;
import beans.AuthenticatedRequestBean;
import exceptions.AsyncRetryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.api.persistence.UserSqlSandbox;
import org.commcare.modern.database.TableBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Factory that determines the correct URL endpoint based on domain, host, and username/asUsername,
 * then retrieves and returns the restore XML.
 */
@Component
public class RestoreFactory {
    @Value("${commcarehq.host}")
    private String host;

    private String asUsername;
    private String username;
    private String domain;
    private HqAuth hqAuth;

    private final Log log = LogFactory.getLog(RestoreFactory.class);

    private String cachedRestore = null;

    public void configure(AuthenticatedRequestBean authenticatedRequestBean, HqAuth auth) {
        configure(authenticatedRequestBean.getUsername(),
                authenticatedRequestBean.getDomain(),
                authenticatedRequestBean.getRestoreAs(),
                auth);
    }

    public void configure(String username, String domain, String asUsername, HqAuth auth) {
        this.setUsername(username);
        this.setDomain(domain);
        this.setAsUsername(asUsername);
        this.setHqAuth(auth);
        cachedRestore = null;
    }

    public String getDbFile() {
        if (getAsUsername() == null) {
            log.info("Restoring to database " + SQLiteProperties.getDataDir() + getDomain() + "/" + getUsername() + ".db");
            return SQLiteProperties.getDataDir() + getDomain() + "/" + getUsername() + ".db";
        }
        log.info("Restoring to database " + SQLiteProperties.getDataDir() + getDomain() + "/" + getUsername() + "/" + getAsUsername() + ".db");
        return SQLiteProperties.getDataDir() + getDomain() + "/" + getUsername() + "/" + getAsUsername() + ".db";
    }

    public String getDbPath() {
        if (asUsername == null) {
            return SQLiteProperties.getDataDir() + domain;
        }
        return SQLiteProperties.getDataDir() + domain + "/" + username;
    }

    public String getWrappedUsername() {
        return asUsername == null ? username : asUsername;
    }

    public UserSqlSandbox getSqlSandbox() {
        return new UserSqlSandbox(getWrappedUsername(), getDbPath());
    }

    private void ensureValidParameters() {
        if (domain == null || (username == null && asUsername == null)) {
            throw new RuntimeException("Domain and one of username or asUsername must be non-null. " +
                    " Domain: " + domain +
                    ", username: " + username +
                    ", asUsername: " + asUsername);
        }
    }

    public String getRestoreXml() {
        return getRestoreXml(false);
    }

    public String getRestoreXml(boolean overwriteCache) {
        if (cachedRestore != null) {
            return cachedRestore;
        }
        ensureValidParameters();

        String restoreUrl;
        if (asUsername == null) {
            restoreUrl = getRestoreUrl(host, domain, overwriteCache);
        } else {
            restoreUrl = getRestoreUrl(host, domain, asUsername, overwriteCache);
        }

        log.info("Restoring from URL " + restoreUrl);
        cachedRestore = getRestoreXmlHelper(restoreUrl, hqAuth);
        return cachedRestore;
    }

    /**
     * Given an async restore xml response, this function throws an AsyncRetryException
     * with meta data about the async restore.
     *
     * @param xml - Async restore response
     * @param headers - HttpHeaders from the restore response
     */
    private void handleAsyncRestoreResponse(String xml, HttpHeaders headers) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        ByteArrayInputStream input;
        Document doc;

        // Create the XML Document builder
        try {
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Unable to instantiate document builder");
        }

        // Parse the xml into a utf-8 byte array
        try {
            input = new ByteArrayInputStream(xml.getBytes("utf-8") );
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to parse async restore response.");
        }

        // Build an XML document
        try {
            doc = builder.parse(input);
        } catch (SAXException e) {
            throw new RuntimeException("Unable to parse into XML Document");
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse into XML Document");
        }

        NodeList messageNodes = doc.getElementsByTagName("message");
        NodeList progressNodes = doc.getElementsByTagName("progress");

        assert messageNodes.getLength() == 1;
        assert progressNodes.getLength() == 1;

        String message = messageNodes.item(0).getTextContent();
        Node progressNode = progressNodes.item(0);
        NamedNodeMap attributes = progressNode.getAttributes();

        throw new AsyncRetryException(
                message,
                Integer.parseInt(attributes.getNamedItem("done").getTextContent()),
                Integer.parseInt(attributes.getNamedItem("total").getTextContent()),
                Integer.parseInt(headers.get("retry-after").get(0))
        );
    }

    private String getRestoreXmlHelper(String restoreUrl, HqAuth auth) {
        RestTemplate restTemplate = new RestTemplate();
        log.info("Restoring at domain: " + domain + " with auth: " + auth);
        HttpHeaders headers = auth.getAuthHeaders();
        headers.add("x-openrosa-version",  "2.0");
        ResponseEntity<String> response = restTemplate.exchange(
                restoreUrl,
                HttpMethod.GET,
                new HttpEntity<String>(headers),
                String.class
        );
        if (response.getStatusCode().value() == 202) {
            handleAsyncRestoreResponse(response.getBody(), response.getHeaders());
        }
        return response.getBody();
    }

    public static String getRestoreUrl(String host, String domain, boolean overwriteCache){
        String url = host + "/a/" + domain + "/phone/restore/?version=2.0";
        if (overwriteCache) {
            url += "&overwrite_cache=true";
        }
        return url;
    }

    public String getRestoreUrl(String host, String domain, String username, boolean overwriteCache) {
        String url = host + "/a/" + domain + "/phone/restore/?as=" + username + "@" +
                domain + ".commcarehq.org&version=2.0";

        if (overwriteCache) {
            url += "&overwrite_cache=true";
        }
        return url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = TableBuilder.scrubName(username);
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public HqAuth getHqAuth() {
        return hqAuth;
    }

    public void setHqAuth(HqAuth hqAuth) {
        this.hqAuth = hqAuth;
    }

    public String getAsUsername() {
        return asUsername;
    }

    public void setAsUsername(String asUsername) {
        this.asUsername = asUsername;
    }

    public void setCachedRestore(String cachedRestore) {
        this.cachedRestore = cachedRestore;
    }
}
