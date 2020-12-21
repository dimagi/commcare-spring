package org.commcare.formplayer.session;

import datadog.trace.api.Trace;
import org.commcare.formplayer.beans.NotificationMessage;
import org.commcare.formplayer.engine.FormplayerConfigEngine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.commcare.modern.database.TableBuilder;
import org.commcare.modern.session.SessionWrapper;
import org.commcare.modern.util.Pair;
import org.commcare.resources.model.InstallCancelledException;
import org.commcare.resources.model.ResourceInitializationException;
import org.commcare.resources.model.UnresolvedResourceException;
import org.commcare.session.CommCareSession;
import org.commcare.session.SessionFrame;
import org.commcare.suite.model.EntityDatum;
import org.commcare.suite.model.FormIdDatum;
import org.commcare.suite.model.SessionDatum;
import org.commcare.util.CommCarePlatform;
import org.commcare.util.screen.*;
import org.commcare.util.screen.MenuScreen;
import org.javarosa.core.model.FormDef;
import org.javarosa.core.model.actions.FormSendCalloutHandler;
import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.core.model.condition.HereFunctionHandlerListener;
import org.javarosa.core.util.MD5;
import org.javarosa.core.util.OrderedHashtable;
import org.javarosa.core.util.externalizable.DeserializationException;
import org.javarosa.xml.util.UnfullfilledRequirementsException;
import org.javarosa.xpath.XPathParseTool;
import org.javarosa.xpath.expr.FunctionUtils;
import org.javarosa.xpath.expr.XPathExpression;
import org.javarosa.xpath.parser.XPathSyntaxException;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Component;
import org.commcare.formplayer.repo.SerializableMenuSession;
import org.commcare.formplayer.sandbox.UserSqlSandbox;
import org.commcare.formplayer.screens.FormplayerQueryScreen;
import org.commcare.formplayer.screens.FormplayerSyncScreen;
import org.commcare.formplayer.services.FormplayerStorageFactory;
import org.commcare.formplayer.services.InstallService;
import org.commcare.formplayer.services.RestoreFactory;
import org.commcare.formplayer.util.*;
import org.commcare.formplayer.util.SessionUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;


/**
 * This (along with FormSession) is a total god object. This manages everything from installation to form entry. This
 * primarily includes module and form navigation, along with case list/details and case selection. When ready,
 * this object will create and hand off flow control to a FormSession object, loading up the proper session data.
 *
 * A lot of this is copied from the CLI. We need to merge that. Big TODO
 */
public class MenuSession implements HereFunctionHandlerListener {
    private FormplayerConfigEngine engine;
    private UserSqlSandbox sandbox;
    private SessionWrapper sessionWrapper;
    private String installReference;
    private final String username;
    private final String domain;

    private String locale;
    private String uuid;
    private String asUser;

    private final Log log = LogFactory.getLog(MenuSession.class);
    private String appId;
    private boolean oneQuestionPerScreen;
    private boolean preview;
    ArrayList<String> breadcrumbs;
    private ArrayList<String> selections = new ArrayList<>();

    private String currentBrowserLocation;
    private boolean hereFunctionEvaluated;

    // Stores the entity screens created to manage state for the lifecycle of this request
    private Map<String, EntityScreen> entityScreenCache = new HashMap<>();

    public MenuSession(SerializableMenuSession session, InstallService installService,
                       RestoreFactory restoreFactory, String host) throws Exception {
        this.username = TableBuilder.scrubName(session.getUsername());
        this.domain = session.getDomain();
        this.asUser = session.getAsUser();
        this.locale = session.getLocale();
        this.uuid = session.getId();
        this.installReference = session.getInstallReference();
        resolveInstallReference(installReference, appId, host);
        this.engine = installService.configureApplication(this.installReference, session.getPreview()).first;
        this.sandbox = restoreFactory.getSandbox();
        this.sessionWrapper = new FormplayerSessionWrapper(deserializeSession(engine.getPlatform(), session.getCommcareSession()),
                engine.getPlatform(), sandbox);
        SessionUtils.setLocale(this.locale);
        sessionWrapper.syncState();
        this.appId = session.getAppId();
        initializeBreadcrumbs();
    }

    public MenuSession(String username, String domain, String appId, String installReference, String locale,
                       InstallService installService, RestoreFactory restoreFactory, String host,
                       boolean oneQuestionPerScreen, String asUser, boolean preview) throws Exception {
        this.username = TableBuilder.scrubName(username);
        this.domain = domain;
        this.appId = appId;
        this.asUser = asUser;
        resolveInstallReference(installReference, appId, host);
        Pair<FormplayerConfigEngine, Boolean> install = installService.configureApplication(this.installReference, preview);
        this.engine = install.first;
        if (install.second && !preview && !restoreFactory.getHasRestored()) {
            this.sandbox = restoreFactory.performTimedSync();
        }
        this.sandbox = restoreFactory.getSandbox();
        this.sessionWrapper = new FormplayerSessionWrapper(engine.getPlatform(), sandbox);
        this.locale = locale;
        SessionUtils.setLocale(this.locale);
        this.uuid = UUID.randomUUID().toString();
        this.oneQuestionPerScreen = oneQuestionPerScreen;
        initializeBreadcrumbs();
        this.preview = preview;
    }

    public void resetSession() {
        this.sessionWrapper = new FormplayerSessionWrapper(engine.getPlatform(), sandbox);
        clearEntityScreenCache();
        initializeBreadcrumbs();
    }

    private void initializeBreadcrumbs() {
        this.breadcrumbs = new ArrayList<>();
        this.breadcrumbs.add(SessionUtils.getAppTitle());
    }

    public NotificationMessage updateApp(String updateMode) {
        try {
            if (this.engine.attemptAppUpdate(updateMode)) {
                return new NotificationMessage("Application updated successfully.", false, NotificationMessage.Tag.update);
            } else {
                return new NotificationMessage("Application up to date.", false, NotificationMessage.Tag.update);
            }
        } catch (UnresolvedResourceException e) {
            String message = "Update Failed! Couldn't find or install one of the remote resources";
            log.error(message, e);
            return new NotificationMessage(message, true, NotificationMessage.Tag.update);
        } catch (UnfullfilledRequirementsException e) {
            String message = "Update Failed! Formplayer is incompatible with the app";
            log.error(message, e);
            return new NotificationMessage(message, true, NotificationMessage.Tag.update);
        } catch (InstallCancelledException e) {
            String message = "Update Failed! Update was cancelled";
            log.error(message, e);
            return new NotificationMessage(message, true, NotificationMessage.Tag.update);
        } catch (ResourceInitializationException e) {
            String message = "Update Failed! Couldn't initialize one of the resources";
            log.error(message, e);
            return new NotificationMessage(message, true, NotificationMessage.Tag.update);
        }
    }

    private void resolveInstallReference(String installReference, String appId, String host){
        if (installReference == null || installReference.equals("")) {
            if(appId == null || "".equals(appId)){
                throw new RuntimeException("Can't install - either installReference or app_id must be non-null");
            }
            this.installReference = host + getReferenceToLatest(appId);
        } else {
            this.installReference = installReference;
        }
    }

    /**
     * Given an app id this returns a URI that will return a CCZ from HQ
     * @param appId An id of the application of the CCZ needed
     * @return      An HQ URI to download the CCZ
     */
    private String getReferenceToLatest(String appId) {
        URIBuilder builder;
        try {
            builder = new URIBuilder("/a/" + this.domain + "/apps/api/download_ccz/");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to instantiate URIBuilder");
        }
        builder.addParameter("app_id", appId);
        builder.addParameter("latest", Constants.CCZ_LATEST_SAVED);
        return builder.toString();
    }

    public boolean handleInput(String input) throws CommCareSessionException {
        return handleInput(input, true, false, false);
    }

    /**
     * @param input             The user step input
     * @param needsDetail       Whether a full entity screen is required for this request
     *                          or if a list of references is sufficient
     * @param confirmed         Whether the input has been previously validated,
     *                          allowing this step to skip validation
     * @param allowAutoLaunch   If this step is allow to automatically launch an action,
     *                          assuming it has an autolaunch action specified.
     */
    @Trace
    public boolean handleInput(String input, boolean needsDetail, boolean confirmed, boolean allowAutoLaunch) throws CommCareSessionException {
        Screen screen = getNextScreen(needsDetail, allowAutoLaunch);
        log.info("Screen " + screen + " handling input " + input);
        if(screen == null) {
            return false;
        }
        try {
            if (screen instanceof EntityScreen) {
                log.info("This is an entity screen, input = " + input + " and confirmed = " + confirmed);
                if (input.startsWith("action ") || !confirmed) {
                    EntityScreen entityScreen = (EntityScreen)screen;
                    if (input.startsWith("action ") && entityScreen.getAutoLaunchAction() != null) {
                        sessionWrapper.executeStackOperations(entityScreen.getAutoLaunchAction().getStackOperations(), entityScreen.getEvalContext());
                    } else {
                        screen.init(sessionWrapper);
                        if (screen.shouldBeSkipped()) {
                            return handleInput(input, true, confirmed, allowAutoLaunch);
                        }
                        screen.handleInputAndUpdateSession(sessionWrapper, input);
                    }
                } else {
                    log.info("Calling setDatum");
                    sessionWrapper.setDatum(sessionWrapper.getNeededDatum().getDataId(), input);
                }
            } else {
                log.info("This is not an entity screen");
                boolean ret = screen.handleInputAndUpdateSession(sessionWrapper, input);
            }
            Screen previousScreen = screen;
            screen = getNextScreen(needsDetail, allowAutoLaunch);
            addTitle(input, previousScreen);
            return true;
        } catch(ArrayIndexOutOfBoundsException | NullPointerException e) {
            throw new RuntimeException("Screen " + screen + "  handling input " + input +
                    " threw exception " + e.getMessage() + ". Please try reloading this application" +
                    " and if the problem persists please report a bug.", e);
        }
    }

    private void addTitle(String input, Screen previousScreen) {
        if (previousScreen instanceof EntityScreen) {
            try {
                String caseName = SessionUtils.tryLoadCaseName(sandbox.getCaseStorage(), input);
                if (caseName != null) {
                    breadcrumbs.add(caseName);
                    return;
                }
            } catch (NoSuchElementException e) {
                // That's ok, just fallback quietly
            }
        }
        breadcrumbs.add(SessionUtils.getBestTitle(getSessionWrapper()));
    }

    public Screen getNextScreen() throws CommCareSessionException {
        return getNextScreen(true, false);
    }

    @Trace
    public Screen getNextScreen(boolean needsDetail) throws CommCareSessionException {
        return getNextScreen(needsDetail, false);
    }

    public Screen getNextScreen(boolean needsDetail, boolean allowAutoLaunch) throws CommCareSessionException {
        String next = sessionWrapper.getNeededData(sessionWrapper.getEvaluationContext());

        if (next == null) {
            if (sessionWrapper.isViewCommand(sessionWrapper.getCommand())) {
                sessionWrapper.stepBack();
                return getNextScreen();
            }
            //XFORM TIME!
            return null;
        } else if (next.equals(SessionFrame.STATE_COMMAND_ID)) {
            MenuScreen menuScreen = new MenuScreen();
            menuScreen.init(sessionWrapper);
            return menuScreen;
        } else if (next.equals(SessionFrame.STATE_DATUM_VAL)) {
            EntityScreen entityScreen = getEntityScreenForSession(needsDetail, allowAutoLaunch);
            if (entityScreen.shouldBeSkipped()) {
                return getNextScreen();
            }
            return entityScreen;
        } else if (next.equalsIgnoreCase(SessionFrame.STATE_DATUM_COMPUTED)) {
            computeDatum();
            return getNextScreen();
        } else if (next.equalsIgnoreCase(SessionFrame.STATE_QUERY_REQUEST)) {
            QueryScreen queryScreen = new FormplayerQueryScreen();
            queryScreen.init(sessionWrapper);
            return queryScreen;
        } else if (next.equalsIgnoreCase(SessionFrame.STATE_SYNC_REQUEST)) {
            String username = asUser != null ?
                    StringUtils.getFullUsername(asUser, domain) : null;
            FormplayerSyncScreen syncScreen = new FormplayerSyncScreen(username);
            syncScreen.init(sessionWrapper);
            return syncScreen;
        }
        throw new RuntimeException("Unexpected Frame Request: " + sessionWrapper.getNeededData());
    }

    private void clearEntityScreenCache() {
        entityScreenCache.clear();
    }

<<<<<<< HEAD
    @Trace
    private EntityScreen getEntityScreenForSession(boolean needsDetail) throws CommCareSessionException {
=======
    private EntityScreen getEntityScreenForSession(boolean needsDetail, boolean allowAutoLaunch) throws CommCareSessionException {
>>>>>>> master
        EntityDatum datum = (EntityDatum)sessionWrapper.getNeededDatum();

        //This is only needed because with remote queries there can be nested datums with the same
        //datum ID in the same http request lifecycle.
        String nodesetHash = MD5.toHex(MD5.hash(datum.getNodeset().toString(true).getBytes()));

        String datumKey = datum.getDataId() + ", "+ nodesetHash;
        if (!entityScreenCache.containsKey(datumKey)) {
            EntityScreen entityScreen = createFreshEntityScreen(needsDetail, allowAutoLaunch);
            entityScreenCache.put(datumKey, entityScreen);
            return entityScreen;
        } else {
            return entityScreenCache.get(datumKey);
        }
    }

<<<<<<< HEAD
    @Trace
    private EntityScreen createFreshEntityScreen(boolean needsDetail) throws CommCareSessionException {
        EntityScreen entityScreen = new EntityScreen(false, needsDetail, sessionWrapper);
=======
    private EntityScreen createFreshEntityScreen(boolean needsDetail, boolean allowAutoLaunch) throws CommCareSessionException {
        EntityScreen entityScreen = new EntityScreen(false, needsDetail, allowAutoLaunch, sessionWrapper);
>>>>>>> master
        return entityScreen;
    }

    private void computeDatum() {
        //compute
        SessionDatum datum = sessionWrapper.getNeededDatum();
        XPathExpression form;
        try {
            form = XPathParseTool.parseXPath(datum.getValue());
        } catch (XPathSyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        EvaluationContext ec = sessionWrapper.getEvaluationContext();
        if (datum instanceof FormIdDatum) {
            sessionWrapper.setXmlns(FunctionUtils.toString(form.eval(ec)));
            sessionWrapper.setDatum("", "awful");
        } else {
            sessionWrapper.setDatum(datum.getDataId(), FunctionUtils.toString(form.eval(ec)));
        }
    }

    @Trace
    private HashMap<String, String> getSessionData() {
        OrderedHashtable<String, String> sessionData = sessionWrapper.getData();
        HashMap<String, String> ret = new HashMap<>();
        for (String key : sessionData.keySet()) {
            ret.put(key, sessionData.get(key));
        }
        return ret;
    }

    @Trace
    public FormSession getFormEntrySession(FormSendCalloutHandler formSendCalloutHandler,
                                           FormplayerStorageFactory storageFactory) throws Exception {
        String formXmlns = sessionWrapper.getForm();
        FormDef formDef = engine.loadFormByXmlns(formXmlns);
        HashMap<String, String> sessionData = getSessionData();
        String postUrl = sessionWrapper.getPlatform().getPropertyManager().getSingularProperty("PostURL");
        return new FormSession(sandbox, formDef, username, domain,
                sessionData, postUrl, locale, uuid,
                null, oneQuestionPerScreen,
                asUser, appId, null, formSendCalloutHandler, storageFactory, false, null);
    }

    public void reloadSession(FormSession formSession) throws Exception {
        String formXmlns = formSession.getXmlns();
        FormDef formDef = engine.loadFormByXmlns(formXmlns);
        String postUrl = sessionWrapper.getPlatform().getPropertyManager().getSingularProperty("PostURL");
        formSession.reload(formDef, postUrl, engine.getPlatform().getStorageManager());
    }

    private byte[] serializeSession(CommCareSession session){
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream oos;
        try {
            oos = new DataOutputStream(baos);
            session.serializeSessionState(oos);
        } catch(IOException e){
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    private CommCareSession deserializeSession(CommCarePlatform platform, byte[] bytes) throws DeserializationException, IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        return CommCareSession.restoreSessionFromStream(platform, in);
    }

    public SessionWrapper getSessionWrapper (){
        return sessionWrapper;
    }

    public String getId() {
        return uuid;
    }

    public void setId(String uuid) {
        this.uuid = uuid;
    }

    public String getUsername() {
        return username;
    }

    public String getDomain() {
        return domain;
    }

    public String getAppId() {
        return appId;
    }

    public String getCommCareVersionString() {
        return sessionWrapper.getIIF().getVersionString();
    }

    public String getAppVersion() {
        return "" + this.engine.getPlatform().getCurrentProfile().getVersion();
    }

    public String getInstallReference() {
        return installReference;
    }

    public byte[] getCommcareSession(){
        return serializeSession(sessionWrapper);
    }

    public String getLocale() {
        return locale;
    }

    public String getAsUser() {
        return asUser;
    }

    public void setAsUser(String asUser) {
        this.asUser = asUser;
    }

    public boolean isOneQuestionPerScreen() {
        return oneQuestionPerScreen;
    }

    public void setOneQuestionPerScreen(boolean oneQuestionPerScreen) {
        this.oneQuestionPerScreen = oneQuestionPerScreen;
    }

    public String[] getBreadcrumbs() {
        String[] ret = new String[breadcrumbs.size()];
        breadcrumbs.toArray(ret);
        return ret;
    }

    public boolean getPreview() {
        return preview;
    }

    public void setPreview(boolean preview) {
        this.preview = preview;
    }

    public String[] getSelections() {
        String[] ret = new String[selections.size()];
        selections.toArray(ret);
        return ret;
    }

    public void addSelection(String currentStep) {
        selections.add(currentStep);
    }

    public void setCurrentBrowserLocation(String location) {
        this.currentBrowserLocation = location;
    }

    public String getCurrentBrowserLocation() {
        return this.currentBrowserLocation;
    }

    @Override
    public void onEvalLocationChanged() {
    }

    @Override
    public void onHereFunctionEvaluated() {
        this.hereFunctionEvaluated = true;
    }

    public boolean locationRequestNeeded() {
        return this.hereFunctionEvaluated && this.currentBrowserLocation == null;
    }

    public boolean hereFunctionEvaluated() {
        return this.hereFunctionEvaluated;
    }

    @Trace
    public EvaluationContext getEvalContextWithHereFuncHandler() {
        EvaluationContext ec = sessionWrapper.getEvaluationContext();
        ec.addFunctionHandler(new FormplayerHereFunctionHandler(this, currentBrowserLocation));
        return ec;
    }
}
