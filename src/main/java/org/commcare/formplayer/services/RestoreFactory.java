package org.commcare.formplayer.services;

import static org.commcare.formplayer.util.Constants.TOGGLE_INCLUDE_STATE_HASH;

import com.google.common.collect.ImmutableMap;
import com.timgroup.statsd.StatsDClient;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.cases.util.CaseDBUtils;
import org.commcare.cases.util.InvalidCaseGraphException;
import org.commcare.core.parse.ParseUtils;
import org.commcare.formplayer.DbUtils;
import org.commcare.formplayer.api.process.FormRecordProcessorHelper;
import org.commcare.formplayer.auth.HqAuth;
import org.commcare.formplayer.beans.AuthenticatedRequestBean;
import org.commcare.formplayer.beans.auth.FeatureFlagChecker;
import org.commcare.formplayer.engine.FormplayerTransactionParserFactory;
import org.commcare.formplayer.exceptions.AsyncRetryException;
import org.commcare.formplayer.exceptions.SQLiteRuntimeException;
import org.commcare.formplayer.exceptions.SyncRestoreException;
import org.commcare.formplayer.sandbox.JdbcSqlStorageIterator;
import org.commcare.formplayer.sandbox.UserSqlSandbox;
import org.commcare.formplayer.sqlitedb.SQLiteDB;
import org.commcare.formplayer.sqlitedb.UserDB;
import org.commcare.formplayer.util.Constants;
import org.commcare.formplayer.util.FormplayerSentry;
import org.commcare.formplayer.util.RequestUtils;
import org.commcare.formplayer.util.SimpleTimer;
import org.commcare.formplayer.util.UserUtils;
import org.commcare.formplayer.web.client.WebClient;
import org.commcare.modern.database.TableBuilder;
import org.javarosa.core.api.ClassNameHasher;
import org.javarosa.core.model.User;
import org.javarosa.core.util.PropertyUtils;
import org.javarosa.core.util.externalizable.PrototypeFactory;
import org.javarosa.xml.util.InvalidStructureException;
import org.javarosa.xml.util.UnfullfilledRequirementsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import datadog.trace.api.Trace;
import io.sentry.SentryLevel;


/**
 * Factory that determines the correct URL endpoint based on domain, host, and username/asUsername,
 * then retrieves and returns the restore XML.
 */
@Component
@Scope(value = "request", proxyMode = ScopedProxyMode.TARGET_CLASS)
public class RestoreFactory {
    @Value("${commcarehq.restore.url}")
    private String restoreUrl;

    @Value("${commcarehq.restore.url.case}")
    private String caseRestoreUrl;

    private String asUsername;
    private String username;
    private String scrubbedUsername;
    private String domain;
    private HqAuth hqAuth;

    private boolean permitAggressiveSyncs = true;

    public static final String FREQ_DAILY = "freq-daily";
    public static final String FREQ_WEEKLY = "freq-weekly";
    public static final String FREQ_NEVER = "freq-never";

    public static final Long FIVE_MINUTES_IN_MILLISECONDS = 1000L * 60 * 5;
    public static final Long ONE_DAY_IN_MILLISECONDS = 86400000l;
    public static final Long ONE_WEEK_IN_MILLISECONDS = ONE_DAY_IN_MILLISECONDS * 7;

    private static final String DEVICE_ID_SLUG = "WebAppsLogin";

    private static final String ORIGIN_TOKEN_SLUG = "OriginToken";

    @Autowired
    protected StatsDClient datadogStatsDClient;

    @Autowired
    private ResponseMetaDataTracker responseMetaDataTracker;

    @Autowired
    private CategoryTimingHelper categoryTimingHelper;

    @Autowired
    private FormplayerStorageFactory storageFactory;

    @Autowired
    private WebClient webClient;

    @Autowired
    private RedisTemplate redisTemplateLong;

    @Resource(name = "redisTemplateLong")
    private ValueOperations<String, Long> valueOperations;

    @Autowired
    private RedisTemplate redisTemplateString;

    @Resource(name = "redisTemplateString")
    private ValueOperations<String, String> originTokens;

    @Autowired
    private RedisTemplate redisSetTemplate;

    @Resource(name = "redisSetTemplate")
    private SetOperations<String, String> redisSessionCache;

    @Value("${commcarehq.formplayerAuthKey}")
    private String formplayerAuthKey;

    private final Log log = LogFactory.getLog(RestoreFactory.class);

    CategoryTimingHelper.RecordingTimer downloadRestoreTimer;

    private SQLiteDB sqLiteDB = new SQLiteDB(null);
    private boolean hasRestored;
    private String caseId;
    private boolean configured = false;

    public void configure(String domain, String caseId, HqAuth auth) {
        this.setUsername(UserUtils.getRestoreAsCaseIdUsername(caseId));
        this.setDomain(domain);
        this.setCaseId(caseId);
        this.setHqAuth(auth);
        this.hasRestored = false;
        this.configured = true;
        sqLiteDB = new UserDB(domain, scrubbedUsername, null);
        log.info(String.format("configuring RestoreFactory with CaseID with arguments " +
                "username = %s, caseId = %s, domain = %s", username, caseId, domain));
    }

    public void configure(String username, String domain, String asUsername, HqAuth auth) {
        this.setUsername(username);
        this.setDomain(domain);
        this.setAsUsername(asUsername);
        this.hqAuth = auth;
        this.hasRestored = false;
        this.configured = true;
        sqLiteDB = new UserDB(domain, scrubbedUsername, asUsername);
        log.info(String.format("configuring RestoreFactory with arguments " +
                "username = %s, asUsername = %s, domain = %s", username, asUsername, domain));
    }

    public void configure(AuthenticatedRequestBean authenticatedRequestBean, HqAuth auth) {
        this.setUsername(authenticatedRequestBean.getUsername());
        this.setDomain(authenticatedRequestBean.getDomain());
        this.setAsUsername(authenticatedRequestBean.getRestoreAs());
        this.setHqAuth(auth);
        this.hasRestored = false;
        this.configured = true;
        sqLiteDB = new UserDB(domain, scrubbedUsername, asUsername);
        log.info(String.format("configuring RestoreFactory from authed request with arguments " +
                        "username = %s, asUsername = %s, domain = %s",
                username, asUsername, domain));
    }

    // This function will only wipe user DBs when they have expired, otherwise will incremental sync
    public UserSqlSandbox performTimedSync() throws SyncRestoreException {
        return performTimedSync(true, false, false);
    }
    public UserSqlSandbox performTimedSync(boolean shouldPurge, boolean skipFixtures, boolean isResponseTo412)
            throws SyncRestoreException {
        return performTimedSync(shouldPurge, skipFixtures, isResponseTo412, new HashMap<>());
    }

    public UserSqlSandbox performTimedSync(boolean shouldPurge, boolean skipFixtures, boolean isResponseTo412, Map<String, String> extraTags)
            throws SyncRestoreException {
        // create extras to send to category timing helper
        extraTags.put(Constants.DOMAIN_TAG, domain);

        SimpleTimer completeRestoreTimer = new SimpleTimer();
        completeRestoreTimer.start();
        // Create parent dirs if needed
        if (getSqlSandbox().getLoggedInUser() != null) {
            getSQLiteDB().createDatabaseFolder();
        }
        UserSqlSandbox sandbox;
        try {
            sandbox = restoreUser(skipFixtures);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 412) {
                return handle412Sync(shouldPurge, skipFixtures);
            }
            throw e;
        }
        if (shouldPurge && sandbox != null) {
            try {
                SimpleTimer purgeTimer = new SimpleTimer();
                purgeTimer.start();
                FormRecordProcessorHelper.purgeCases(sandbox);
                purgeTimer.end();
                categoryTimingHelper.recordCategoryTiming(
                        purgeTimer,
                        Constants.TimingCategories.PURGE_CASES,
                        null,
                        extraTags
                );
            } catch (InvalidCaseGraphException e) {
                FormplayerSentry.captureException(e, SentryLevel.WARNING);
                // if we have not already, do a fresh sync to try and resolve state
                if (!isResponseTo412) {
                    handle412Sync(shouldPurge, skipFixtures);
                } else {
                    // there are cycles even after a fresh sync
                    throw new SyncRestoreException(e);
                }
            }
        }
        completeRestoreTimer.end();
        categoryTimingHelper.recordCategoryTiming(
                completeRestoreTimer,
                Constants.TimingCategories.COMPLETE_RESTORE,
                null,
                extraTags
        );
        return sandbox;
    }

    private UserSqlSandbox handle412Sync(boolean shouldPurge, boolean skipFixtures) throws SyncRestoreException {
        getSQLiteDB().deleteDatabaseFile();
        // this line has the effect of clearing the sync token
        // from the restore URL that's used
        sqLiteDB = new UserDB(domain, scrubbedUsername, asUsername);
        return performTimedSync(shouldPurge, skipFixtures, true);
    }

    // This function will attempt to get the user DBs without syncing if they exist, sync if not
    @Trace
    public UserSqlSandbox getSandbox() throws SyncRestoreException {
        if (getSqlSandbox().getLoggedInUser() != null
                && !isRestoreXmlExpired()) {
            return getSqlSandbox();
        } else {
            getSQLiteDB().createDatabaseFolder();
            return performTimedSync(true, false, false);
        }
    }

    @Trace
    private UserSqlSandbox restoreUser(boolean skipFixtures) throws SyncRestoreException {
        PrototypeFactory.setStaticHasher(new ClassNameHasher());

        // create extras to send to category timing helper
        Map<String, String> extras = new HashMap<>();
        extras.put(Constants.DOMAIN_TAG, domain);

        int maxRetries = 2;
        int counter = 0;
        while (true) {
            try {
                UserSqlSandbox sandbox = getSqlSandbox();
                FormplayerTransactionParserFactory factory = new FormplayerTransactionParserFactory(sandbox, true);
                InputStream restoreStream = getRestoreXml(skipFixtures);

                SimpleTimer parseTimer = new SimpleTimer();
                parseTimer.start();

                setAutoCommit(false);
                ParseUtils.parseIntoSandbox(restoreStream, factory, true, true);
                hasRestored = true;
                commit();
                setAutoCommit(true);

                parseTimer.end();
                categoryTimingHelper.recordCategoryTiming(
                        parseTimer,
                        Constants.TimingCategories.PARSE_RESTORE,
                        null,
                        extras
                );
                sandbox.writeSyncToken();
                return sandbox;
            } catch (InvalidStructureException | SQLiteRuntimeException e) {
                if (e instanceof InvalidStructureException || ++counter >= maxRetries) {
                    // Before throwing exception, rollback any changes to relinquish SQLite lock
                    rollback();
                    setAutoCommit(true);
                    getSQLiteDB().deleteDatabaseFile();
                    getSQLiteDB().createDatabaseFolder();
                    throw new SyncRestoreException(e);
                } else {
                    log.info(String.format("Retrying restore for user %s after receiving exception.",
                            getEffectiveUsername()),
                            e);
                }
            } catch (UnfullfilledRequirementsException | XmlPullParserException | IOException e) {
                throw new SyncRestoreException(e);
            }
        }
    }

    @Trace
    public UserSqlSandbox getSqlSandbox() {
        return new UserSqlSandbox(this.sqLiteDB);
    }

    public void setAutoCommit(boolean autoCommit) {
        DbUtils.setAutoCommit(sqLiteDB, autoCommit);
    }

    public boolean getAutoCommit() {
        try {
            return sqLiteDB.getConnection().getAutoCommit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        String cacheKey = getSessionCacheKey();
        redisSessionCache.getOperations().delete(cacheKey);
        DbUtils.commit(sqLiteDB);
    }

    public void rollback() {
        DbUtils.rollback(sqLiteDB);
    }

    public SQLiteDB getSQLiteDB() {
        return sqLiteDB;
    }

    public String getWrappedUsername() {
        return asUsername == null ? username : asUsername;
    }

    public String getEffectiveUsername() {
        return UserUtils.getShortUsername(getWrappedUsername(), domain);
    }

    private void ensureValidParameters() {
        if (domain == null || (username == null && asUsername == null)) {
            throw new RuntimeException("Domain and one of username or asUsername must be non-null. " +
                    " Domain: " + domain +
                    ", username: " + username +
                    ", asUsername: " + asUsername);
        }
    }

    public String getSyncFreqency() {
        try {
            return storageFactory.getPropertyManager().getSingularProperty("cc-autosync-freq");
        } catch (RuntimeException e) {
            // In cases where we don't have access to the PropertyManager, such sync-db, this call
            // throws a RuntimeException
            return RestoreFactory.FREQ_NEVER;
        }
    }

    public boolean useAggressiveSyncTiming() {
        try {
            return storageFactory.getPropertyManager().isSyncAfterFormEnabled() &&
                    permitAggressiveSyncs;
        } catch (RuntimeException e) {
            // In cases where we don't have access to the PropertyManager, such as sync-db, this call
            // throws a RuntimeException
            return false;
        }
    }

    /**
     * Based on the frequency of restore set in the app, this method determines
     * whether the user should sync
     *
     * @return boolean - true if restore has expired, false otherwise
     */
    public boolean isRestoreXmlExpired() {
        String freq = getSyncFreqency();
        Long lastSyncTime = getLastSyncTime();
        boolean isAggressive = useAggressiveSyncTiming();

        if (lastSyncTime == null) {
            return isAggressive;
        }

        Long delta = System.currentTimeMillis() - lastSyncTime;

        if (isAggressive) {
            return delta > FIVE_MINUTES_IN_MILLISECONDS;
        }

        if (freq == null) {
            return false;
        }

        switch (freq) {
            case FREQ_DAILY:
                return delta > ONE_DAY_IN_MILLISECONDS;
            case FREQ_WEEKLY:
                return delta > ONE_WEEK_IN_MILLISECONDS;
            case FREQ_NEVER:
                return false;
            default:
                return false;
        }
    }

    @Trace
    public InputStream getRestoreXml(boolean skipFixtures) {
        ensureValidParameters();
        URI url = getRestoreUrl(skipFixtures);
        recordSentryData(url.toString());
        InputStream restoreStream = getRestoreXmlHelper(url);
        setLastSyncTime();
        return restoreStream;
    }

    public HttpHeaders getRequestHeaders(URI url) {
        HttpHeaders headers;
        if (RequestUtils.requestAuthedWithHmac()) {
            headers = getHmacHeader(url);
        } else {
            headers = getHqAuth().getAuthHeaders();;
        }
        headers.addAll(getStandardHeaders());
        return headers;
    }

    private void recordSentryData(final String restoreUrl) {
        FormplayerSentry.newBreadcrumb()
                .setData("restoreUrl", restoreUrl)
                .setCategory("restore")
                .setMessage("Restoring from URL " + restoreUrl)
                .record();
    }

    private void setLastSyncTime() {
        valueOperations.set(lastSyncKey(), System.currentTimeMillis(), 10, TimeUnit.DAYS);
    }

    public Long getLastSyncTime() {
        // valueOperations should only be null when we don't have access to Redis.
        // This currently only happens in tests.
        if (valueOperations == null) {
            return null;
        }
        return valueOperations.get(lastSyncKey());
    }

    private String lastSyncKey() {
        return "last-sync-time:" + domain + ":" + scrubbedUsername + ":" + asUsername;
    }

    /**
     * Given an async restore xml response, this function throws an AsyncRetryException
     * with meta data about the async restore.
     *
     * @param xml     - Async restore response
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
            input = new ByteArrayInputStream(xml.getBytes("utf-8"));
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

    private InputStream getRestoreXmlHelper(URI restoreUrl) {
        ResponseEntity<org.springframework.core.io.Resource> response;
        String status = "error";
        downloadRestoreTimer = categoryTimingHelper.newTimer(Constants.TimingCategories.DOWNLOAD_RESTORE, domain);
        downloadRestoreTimer.start();
        try {
            responseMetaDataTracker.setAttemptRestore(true);
            response = webClient.getRaw(restoreUrl, org.springframework.core.io.Resource.class);
            status = response.getStatusCode().toString();
        } catch (HttpClientErrorException e) {
            status = e.getStatusCode().toString();
            throw e;
        } finally {
            downloadRestoreTimer.end();
            datadogStatsDClient.increment(
                    Constants.DATADOG_RESTORE_COUNT,
                    "domain:" + domain,
                    "duration:" + downloadRestoreTimer.getDurationBucket(),
                    "status:" + status
            );
        }

        // Handle Async restore
        if (response.getStatusCode().value() == 202) {
            String responseBody = null;
            try {
                responseBody = IOUtils.toString(response.getBody().getInputStream(), "utf-8");
            } catch (IOException e) {
                throw new RuntimeException("Unable to read async restore response", e);
            }
            handleAsyncRestoreResponse(responseBody, response.getHeaders());
        }

        InputStream stream = null;
        try {
            stream = response.getBody().getInputStream();
            downloadRestoreTimer.record();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read restore response", e);
        }
        return stream;
    }

    public String getSyncToken() {
        JdbcSqlStorageIterator<User> iterator = getSqlSandbox().getUserStorage().iterate();
        try {
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next().getLastSyncToken();
        } finally {
            iterator.close();
        }
    }

    // Device ID for tracking usage in the same way Android uses IMEI
    private String getSyncDeviceId() {
        if (asUsername == null) {
            return DEVICE_ID_SLUG;
        }
        return String.format("%s*%s*as*%s", DEVICE_ID_SLUG, username, asUsername);
    }

    private HttpHeaders getStandardHeaders() {
        HttpHeaders headers = new HttpHeaders() {
            {
                set("X-OpenRosa-Version", "3.0");
                set("X-OpenRosa-DeviceId", getSyncDeviceId());
            }
        };
        String syncToken = getSyncToken();
        if (syncToken != null) {
            headers.set("X-CommCareHQ-LastSyncToken", getSyncToken());
        }
        headers.setAll(getOriginTokenHeader());
        return headers;
    }

    private Map<String, String> getOriginTokenHeader() {
        String originToken = PropertyUtils.genUUID();
        String redisKey = String.format("%s%s", ORIGIN_TOKEN_SLUG, originToken);
        originTokens.set(redisKey, "valid", Duration.ofSeconds(60));
        return Collections.singletonMap("X-CommCareHQ-Origin-Token", originToken);
    }

    public URI getRestoreUrl(boolean skipFixtures) {
        // TODO: remove timing once the state hash rollout is complete
        return categoryTimingHelper.timed(Constants.TimingCategories.BUILD_RESTORE_URL, () -> {
            if (caseId != null) {
                return getCaseRestoreUrl();
            }
            return getUserRestoreUrl(skipFixtures);
        });
    }

    private HttpHeaders getHmacHeader(URI url) {
        // Do HMAC auth which requires only the path and query components of the URL
        String requestPath = url.getRawPath();
        if (url.getRawQuery() != null) {
            requestPath = String.format("%s?%s", requestPath, url.getRawQuery());
        }
        if (!RequestUtils.requestAuthedWithHmac()) {
            throw new RuntimeException(String.format("Tried getting HMAC Auth for request %s but this request" +
                    "was not validated with HMAC.", requestPath));
        }
        String digest;
        try {
            digest = RequestUtils.getHmac(formplayerAuthKey, requestPath);
        } catch (Exception e) {
            log.error("Could not get HMAC signature to auth restore request", e);
            throw new RuntimeException(e);
        }

        return new HttpHeaders() {
            {
                add("X-MAC-DIGEST", digest);
            }
        };
    }

    public URI getCaseRestoreUrl() {
        return UriComponentsBuilder.fromHttpUrl(caseRestoreUrl).buildAndExpand(domain, caseId).toUri();
    }

    public URI getUserRestoreUrl(boolean skipFixtures) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(restoreUrl).encode();

        Map<String, String> params = new LinkedHashMap<>();
        params.put("version", "2.0");
        params.put("device_id", getSyncDeviceId());

        String syncToken = getSyncToken();
        // Add query params.
        if (syncToken != null && !"".equals(syncToken)) {
            params.put("since", syncToken);
        }

        if (FeatureFlagChecker.isToggleEnabled(TOGGLE_INCLUDE_STATE_HASH)) {
            String caseStateHash = getCaseDbHash();
            if (!caseStateHash.isEmpty()) {
                params.put("state", caseStateHash);
            }
        }

        if (asUsername != null) {
            String asUserParam = asUsername;
            if (!asUsername.contains("@")) {
                asUserParam += "@" + domain + ".commcarehq.org";
            }
            params.put("as", asUserParam);
        } else if (getHqAuth() == null && username != null) {
            // HQ requesting to force a sync for a user
            params.put("as", username);
        }
        if (skipFixtures) {
            params.put("skip_fixtures", "true");
        }

        // add the params to the query builder as templates
        params.forEach((key, value) -> builder.queryParam(key, String.format("{%s}", key)));

        Map<String, String> templateVars = ImmutableMap.<String, String>builder()
                .putAll(params)
                .put("domain", this.domain)
                .build();
        return builder.buildAndExpand(templateVars).toUri();
    }

    public String getCaseDbHash() {
        String hash = CaseDBUtils.computeCaseDbHash(getSqlSandbox().getCaseStorage());
        if (hash.isEmpty()) {
            return "";
        }
        return String.format("ccsh:%s", hash);
    }

    /**
     * Configures whether restores through this factory should support 'aggressive' syncs.
     */
    @Trace
    public void setPermitAggressiveSyncs(boolean permitAggressiveSyncs) {
        this.permitAggressiveSyncs = permitAggressiveSyncs;
    }

    private String getSessionCacheKey() {
        StringBuilder builder = new StringBuilder();
        builder.append(storageFactory.getAppId());
        builder.append("_").append(domain);
        builder.append("_").append(scrubbedUsername);
        if (asUsername != null) {
            builder.append("_").append(asUsername);
        }
        return builder.toString();
    }

    private String getSessionCacheValue(String[] selections) {
        return String.join("|", selections);
    }

    /**
     * Adds a sequence of menu selections to the set of validated selections
     * for a given user session so that certain optimizations can skip validation
     *
     * @param selections - Array of menu selections (e.g. ["1", "1", <case_id>])
     */
    public void cacheSessionSelections(String[] selections) {
        if (!ArrayUtils.isEmpty(selections)) {
            String cacheKey = getSessionCacheKey();
            String cacheValue = getSessionCacheValue(selections);
            redisSessionCache.add(cacheKey, cacheValue);
            redisSetTemplate.expire(cacheKey, 1, TimeUnit.HOURS);
        }
    }

    /**
     * Checks whether a sequence of menu selections has already been validated
     * for a given user session
     *
     * @param selections - Array of menu selections (e.g. ["1", "1", <case_id>])
     */
    public boolean isConfirmedSelection(String[] selections) {
        String cacheKey = getSessionCacheKey();
        String cacheValue = getSessionCacheValue(selections);
        return redisSessionCache.isMember(cacheKey, cacheValue);
    }

    @PreDestroy
    public void preDestroy() {
        if (sqLiteDB != null) {
            sqLiteDB.closeConnection();
        }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
        this.scrubbedUsername = TableBuilder.scrubName(username);
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

    public String getScrubbedUsername() {
        return scrubbedUsername;
    }

    public boolean getHasRestored() {
        return hasRestored;
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    public String getCaseId() {
        return caseId;
    }
}
