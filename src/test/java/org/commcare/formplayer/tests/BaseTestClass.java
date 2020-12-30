package org.commcare.formplayer.tests;

import org.commcare.formplayer.application.*;
import org.commcare.formplayer.auth.DjangoAuth;
import org.commcare.formplayer.beans.*;
import org.commcare.formplayer.beans.debugger.XPathQueryItem;
import org.commcare.formplayer.beans.menus.CommandListResponseBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.commcare.formplayer.installers.FormplayerInstallerFactory;
import org.commcare.formplayer.sandbox.UserSqlSandbox;
import org.commcare.formplayer.sqlitedb.UserDB;
import org.javarosa.core.model.utils.DateUtils;
import org.javarosa.core.model.utils.TimezoneProvider;
import org.javarosa.core.services.locale.LocalizerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.commcare.formplayer.repo.FormSessionRepo;
import org.commcare.formplayer.repo.MenuSessionRepo;
import org.commcare.formplayer.repo.SerializableMenuSession;
import org.commcare.formplayer.sandbox.SqlSandboxUtils;
import org.commcare.formplayer.services.*;
import org.commcare.formplayer.util.Constants;
import org.commcare.formplayer.util.FormplayerSentry;
import org.commcare.formplayer.util.PrototypeUtils;
import org.commcare.formplayer.utils.FileUtils;
import org.commcare.formplayer.utils.TestContext;

import javax.servlet.http.Cookie;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Created by willpride on 2/3/16.
 */
@ContextConfiguration(classes = TestContext.class)
public class BaseTestClass {

    private MockMvc mockFormController;

    private MockMvc mockUtilController;

    private MockMvc mockMenuController;

    private MockMvc mockDebuggerController;

    @Spy
    private StringRedisTemplate redisTemplate;

    @Autowired
    protected FormSessionRepo formSessionRepoMock;

    @Autowired
    private MenuSessionRepo menuSessionRepoMock;

    @Autowired
    private XFormService xFormServiceMock;

    @Autowired
    RestoreFactory restoreFactoryMock;

    @Autowired
    FormplayerStorageFactory storageFactoryMock;

    @Autowired
    SubmitService submitServiceMock;

    @Autowired
    CategoryTimingHelper categoryTimingHelper;

    @Autowired
    private InstallService installService;

    @Autowired
    private NewFormResponseFactory newFormResponseFactoryMock;

    @Autowired
    protected FormplayerSentry ravenMock;

    @Autowired
    protected LockRegistry userLockRegistry;

    @Autowired
    protected FormplayerInstallerFactory formplayerInstallerFactory;

    @Autowired
    protected MenuSessionFactory menuSessionFactory;

    @Autowired
    protected MenuSessionRunnerService menuSessionRunnerService;

    @Autowired
    protected QueryRequester queryRequester;

    @Autowired
    protected SyncRequester syncRequester;

    @InjectMocks
    protected FormController formController;

    @InjectMocks
    protected UtilController utilController;

    @InjectMocks
    protected MenuController menuController;

    @InjectMocks
    protected DebuggerController debuggerController;

    @Mock
    private ListOperations<String, XPathQueryItem> listOperations;

    @Mock
    private ValueOperations<String, Long> valueOperations;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private SetOperations<String, String> redisSessionCache;

    protected ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        Mockito.reset(formSessionRepoMock);
        Mockito.reset(menuSessionRepoMock);
        Mockito.reset(xFormServiceMock);
        Mockito.reset(restoreFactoryMock);
        Mockito.reset(submitServiceMock);
        Mockito.reset(categoryTimingHelper);
        Mockito.reset(installService);
        Mockito.reset(userLockRegistry);
        Mockito.reset(newFormResponseFactoryMock);
        Mockito.reset(storageFactoryMock);
        Mockito.reset(formplayerInstallerFactory);
        Mockito.reset(queryRequester);
        Mockito.reset(syncRequester);
        Mockito.reset(ravenMock);
        Mockito.reset(menuSessionFactory);
        Mockito.reset(menuSessionRunnerService);
        MockitoAnnotations.initMocks(this);
        mockFormController = MockMvcBuilders.standaloneSetup(formController).build();
        mockUtilController = MockMvcBuilders.standaloneSetup(utilController).build();
        mockMenuController = MockMvcBuilders.standaloneSetup(menuController).build();
        mockDebuggerController = MockMvcBuilders.standaloneSetup(debuggerController).build();
        RestoreFactoryAnswer answer = new RestoreFactoryAnswer(this.getMockRestoreFileName());
        Mockito.doAnswer(answer).when(restoreFactoryMock).getRestoreXml(anyBoolean());
        setupSubmitServiceMock();
        Mockito.doReturn(false)
                .when(restoreFactoryMock).isRestoreXmlExpired();
        mapper = new ObjectMapper();
        storageFactoryMock.getSQLiteDB().closeConnection();
        restoreFactoryMock.getSQLiteDB().closeConnection();
        PrototypeUtils.setupThreadLocalPrototypes();
        LocalizerManager.setUseThreadLocalStrategy(true);
        new SQLiteProperties().setDataDir(getDatabaseFolderRoot());
        MockTimezoneProvider tzProvider = new MockTimezoneProvider();
        DateUtils.setTimezoneProvider(tzProvider);
    }

    protected String getDatabaseFolderRoot() {
        return "testdbs/";
    }

    protected boolean removeDatabaseFoldersAfterTests() {
        return true;
    }

    private void setupSubmitServiceMock() {
        Mockito.doReturn(ResponseEntity.ok(
                "<OpenRosaResponse>" +
                        "<message nature='status'>" +
                        "OK" +
                        "</message>" +
                        "</OpenRosaResponse>"))
                .when(submitServiceMock).submitForm(anyString(), anyString());
    }

    @After
    public void tearDown() throws SQLException {
        if(customConnector != null) {
            customConnector.closeConnection();
        }

        if(sandbox != null ) {
            sandbox.getConnection().close();
        }
        restoreFactoryMock.getSQLiteDB().closeConnection();
        storageFactoryMock.getSQLiteDB().closeConnection();
        if (removeDatabaseFoldersAfterTests()) {
            SqlSandboxUtils.deleteDatabaseFolder(SQLiteProperties.getDataDir());
        }
    }

    private UserDB customConnector;

    protected UserDB getUserDbConnector(String domain, String username, String restoreAs) {
        customConnector = new UserDB(domain, username, restoreAs);
        return customConnector;
    }

    UserSqlSandbox sandbox;

    protected UserSqlSandbox getRestoreSandbox() {
        if (sandbox != null) {
            try {
                sandbox.getConnection().close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            sandbox =  null;
        }
        sandbox = restoreFactoryMock.getSqlSandbox();
        return sandbox;
    }

    public class RestoreFactoryAnswer implements Answer {
        private String mRestoreFile;

        public RestoreFactoryAnswer(String restoreFile) {
            mRestoreFile = restoreFile;
        }

        @Override
        public InputStream answer(InvocationOnMock invocation) throws Throwable {
            return new FileInputStream("src/test/resources/" + mRestoreFile);
        }
    }

    private String urlPrepend(String string) {
        return "/" + string;
    }

    protected String getMockRestoreFileName() {
        return "test_restore.xml";
    }

    protected void configureRestoreFactory(String domain, String username) {
        restoreFactoryMock.setDomain(domain);
        restoreFactoryMock.setUsername(username);
    }

    FormEntryNavigationResponseBean nextScreen(String sessionId) throws Exception {
        return nextScreen(sessionId, false);
    }

    FormEntryNavigationResponseBean nextScreen(String sessionId, boolean promptMode) throws Exception {
        SessionRequestBean questionsBean = new SessionRequestBean();
        questionsBean.setSessionId(sessionId);
        questionsBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        questionsBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());

        if (promptMode) {
            return generateMockQuery(ControllerType.FORM,
                    RequestType.POST,
                    Constants.URL_NEXT,
                    questionsBean,
                    FormEntryNavigationResponseBean.class);
        }

        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_NEXT_INDEX,
                questionsBean,
                FormEntryNavigationResponseBean.class);
    }

    FormEntryNavigationResponseBean previousScreen(String sessionId) throws Exception {
        SessionRequestBean questionsBean = new SessionRequestBean();
        questionsBean.setSessionId(sessionId);
        questionsBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        questionsBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_PREV_INDEX,
                questionsBean,
                FormEntryNavigationResponseBean.class);
    }

    FormEntryResponseBean answerQuestionGetResult(String requestPath, String sessionId) throws Exception {
        String requestPayload = FileUtils.getFile(this.getClass(), requestPath);
        AnswerQuestionRequestBean answerQuestionBean = mapper.readValue(requestPayload,
                AnswerQuestionRequestBean.class);
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_ANSWER_QUESTION,
                answerQuestionBean,
                FormEntryResponseBean.class);
    }

    FormEntryResponseBean changeLanguage(String locale) throws Exception {
        ChangeLocaleRequestBean changeLocaleBean = new ChangeLocaleRequestBean();
        changeLocaleBean.setLocale(locale);
        changeLocaleBean.setUsername(formSessionRepoMock.findOneWrapped("sessionid").getUsername());
        changeLocaleBean.setDomain(formSessionRepoMock.findOneWrapped("sessionid").getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_CHANGE_LANGUAGE,
                changeLocaleBean,
                FormEntryResponseBean.class);
    }

    FormEntryResponseBean answerQuestionGetResult(String index, String answer, String sessionId) throws Exception {
        AnswerQuestionRequestBean answerQuestionBean = new AnswerQuestionRequestBean(index, answer, sessionId);
        answerQuestionBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        answerQuestionBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_ANSWER_QUESTION,
                answerQuestionBean,
                FormEntryResponseBean.class);
    }

    GetInstanceResponseBean getInstance(String sessionId) throws Exception {
        SessionRequestBean sessionRequestBean = new SessionRequestBean();
        sessionRequestBean.setSessionId(sessionId);
        sessionRequestBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        sessionRequestBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.GET,
                Constants.URL_GET_INSTANCE,
                sessionRequestBean,
                GetInstanceResponseBean.class);
    }

    NewFormResponse startNewForm(String requestPath, String formPath) throws Exception {
        when(xFormServiceMock.getFormXml(anyString()))
                .thenReturn(FileUtils.getFile(this.getClass(), formPath));
        String requestPayload = FileUtils.getFile(this.getClass(), requestPath);
        NewSessionRequestBean newSessionRequestBean = mapper.readValue(requestPayload,
                NewSessionRequestBean.class);
        when(restoreFactoryMock.getUsername())
                .thenReturn(newSessionRequestBean.getUsername());
        when(restoreFactoryMock.getDomain())
                .thenReturn(newSessionRequestBean.getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_NEW_SESSION,
                newSessionRequestBean,
                NewFormResponse.class);
    }

    SubmitResponseBean submitForm(String sessionId) throws Exception {
        return submitForm(new HashMap<String, Object>(), sessionId);
    }

    SubmitResponseBean submitForm(String requestPath, String sessionId) throws Exception {
        SubmitRequestBean submitRequestBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), SubmitRequestBean.class);
        submitRequestBean.setSessionId(sessionId);
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_SUBMIT_FORM,
                submitRequestBean,
                SubmitResponseBean.class);
    }


    SubmitResponseBean submitForm(Map<String, Object> answers, String sessionId) throws Exception {
        SubmitRequestBean submitRequestBean = new SubmitRequestBean();
        submitRequestBean.setSessionId(sessionId);
        submitRequestBean.setAnswers(answers);
        submitRequestBean.setPrevalidated(true);
        submitRequestBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        submitRequestBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        return generateMockQuery(ControllerType.FORM,
                RequestType.POST,
                Constants.URL_SUBMIT_FORM,
                submitRequestBean,
                SubmitResponseBean.class);
    }

    protected SyncDbResponseBean syncDb() throws Exception {
        SyncDbRequestBean syncDbRequestBean = new SyncDbRequestBean();
        syncDbRequestBean.setDomain(restoreFactoryMock.getDomain());
        syncDbRequestBean.setUsername(restoreFactoryMock.getUsername());
        syncDbRequestBean.setRestoreAs(restoreFactoryMock.getAsUsername());
        return generateMockQuery(ControllerType.UTIL,
                RequestType.POST,
                Constants.URL_SYNC_DB,
                syncDbRequestBean,
                SyncDbResponseBean.class);
    }

    NotificationMessage deleteApplicationDbs() throws Exception {
        String payload = FileUtils.getFile(this.getClass(), "requests/delete_db/delete_db.json");
        DeleteApplicationDbsRequestBean request = mapper.readValue(
                payload,
                DeleteApplicationDbsRequestBean.class
        );

        return generateMockQuery(
                ControllerType.UTIL,
                RequestType.POST,
                Constants.URL_DELETE_APPLICATION_DBS,
                request,
                NotificationMessage.class
        );
    }

    FormEntryResponseBean newRepeatRequest(String sessionId) throws Exception {
        String newRepeatRequestPayload = FileUtils.getFile(this.getClass(), "requests/new_repeat/new_repeat.json");
        RepeatRequestBean newRepeatRequestBean = mapper.readValue(newRepeatRequestPayload,
                RepeatRequestBean.class);
        newRepeatRequestBean.setSessionId(sessionId);
        newRepeatRequestBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        newRepeatRequestBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());

        return generateMockQuery(
                ControllerType.FORM,
                RequestType.POST,
                Constants.URL_NEW_REPEAT,
                newRepeatRequestBean,
                FormEntryResponseBean.class
        );
    }

    FormEntryResponseBean deleteRepeatRequest(String sessionId) throws Exception {

        String newRepeatRequestPayload = FileUtils.getFile(this.getClass(), "requests/delete_repeat/delete_repeat.json");

        RepeatRequestBean deleteRepeatRequest = mapper.readValue(newRepeatRequestPayload,
                RepeatRequestBean.class);
        deleteRepeatRequest.setSessionId(sessionId);
        deleteRepeatRequest.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        deleteRepeatRequest.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        return generateMockQuery(
                ControllerType.FORM,
                RequestType.POST,
                Constants.URL_DELETE_REPEAT,
                deleteRepeatRequest,
                FormEntryResponseBean.class
        );
    }

    EvaluateXPathResponseBean evaluateXPath(String sessionId, String xPath) throws Exception {
        EvaluateXPathRequestBean evaluateXPathRequestBean = mapper.readValue(
                FileUtils.getFile(this.getClass(), "requests/evaluate_xpath/evaluate_xpath.json"),
                EvaluateXPathRequestBean.class
        );
        evaluateXPathRequestBean.setUsername(formSessionRepoMock.findOneWrapped(sessionId).getUsername());
        evaluateXPathRequestBean.setDomain(formSessionRepoMock.findOneWrapped(sessionId).getDomain());
        evaluateXPathRequestBean.setSessionId(sessionId);
        evaluateXPathRequestBean.setXpath(xPath);
        evaluateXPathRequestBean.setDebugOutputLevel(Constants.BASIC_NO_TRACE);
        return generateMockQuery(
                ControllerType.DEBUGGER,
                RequestType.POST,
                Constants.URL_EVALUATE_XPATH,
                evaluateXPathRequestBean,
                EvaluateXPathResponseBean.class
        );
    }

    EvaluateXPathResponseBean evaluateMenuXPath(String requestPath) throws Exception {
        EvaluateXPathMenuRequestBean sessionNavigationBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), EvaluateXPathMenuRequestBean.class);
        return generateMockQuery(
                ControllerType.DEBUGGER,
                RequestType.POST,
                Constants.URL_EVALUATE_MENU_XPATH,
                sessionNavigationBean,
                EvaluateXPathResponseBean.class
        );
    }

    EvaluateXPathResponseBean evaluateMenuXPath(String menuSessionId, String xpath) throws Exception {
        SerializableMenuSession menuSession = menuSessionRepoMock.findOneWrapped(menuSessionId);

        EvaluateXPathMenuRequestBean evaluateXPathRequestBean = new EvaluateXPathMenuRequestBean();
        evaluateXPathRequestBean.setUsername(menuSession.getUsername());
        evaluateXPathRequestBean.setDomain(menuSession.getDomain());
        evaluateXPathRequestBean.setRestoreAs(menuSession.getAsUser());
        evaluateXPathRequestBean.setMenuSessionId(menuSessionId);
        evaluateXPathRequestBean.setXpath(xpath);
        return generateMockQuery(
                ControllerType.DEBUGGER,
                RequestType.POST,
                Constants.URL_EVALUATE_MENU_XPATH,
                evaluateXPathRequestBean,
                EvaluateXPathResponseBean.class
        );
    }

    <T> T getDetails(String requestPath, Class<T> clazz) throws Exception {
        SessionNavigationBean sessionNavigationBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), SessionNavigationBean.class);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_GET_DETAILS,
                sessionNavigationBean,
                clazz);
    }

    <T> T getDetails(String[] selections, String testName, Class<T> clazz) throws Exception {
        return getDetails(selections, testName, null, clazz, false);
    }

    <T> T getDetailsInline(String[] selections, String testName, Class<T> clazz) throws Exception {
        return getDetails(selections, testName, null, clazz, true);
    }

    <T> T getDetails(String[] selections, String testName, String locale, Class<T> clazz, boolean inline) throws Exception {
        SessionNavigationBean sessionNavigationBean = new SessionNavigationBean();
        sessionNavigationBean.setDomain(testName + "domain");
        sessionNavigationBean.setAppId(testName + "appid");
        sessionNavigationBean.setUsername(testName + "username");
        sessionNavigationBean.setInstallReference("archives/" + testName + ".ccz");
        sessionNavigationBean.setSelections(selections);
        sessionNavigationBean.setIsPersistent(inline);
        if (locale != null && !"".equals(locale.trim())) {
            sessionNavigationBean.setLocale(locale);
        }
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_GET_DETAILS,
                sessionNavigationBean,
                clazz);
    }

    <T> T sessionNavigate(String requestPath, Class<T> clazz) throws Exception {
        SessionNavigationBean sessionNavigationBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), SessionNavigationBean.class);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_MENU_NAVIGATION,
                sessionNavigationBean,
                clazz);
    }

    <T> T sessionNavigate(String[] selections, String testName, Class<T> clazz) throws Exception {
        return sessionNavigate(selections, testName, null, clazz);
    }

    <T> T sessionNavigate(String[] selections, String testName, String locale, Class<T> clazz) throws Exception {
        SessionNavigationBean sessionNavigationBean = new SessionNavigationBean();
        sessionNavigationBean.setDomain(testName + "domain");
        sessionNavigationBean.setAppId(testName + "appid");
        sessionNavigationBean.setUsername(testName + "username");
        sessionNavigationBean.setInstallReference("archives/" + testName + ".ccz");
        sessionNavigationBean.setSelections(selections);
        if (locale != null && !"".equals(locale.trim())) {
            sessionNavigationBean.setLocale(locale);
        }
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_MENU_NAVIGATION,
                sessionNavigationBean,
                clazz);
    }

    <T> T sessionNavigate(String[] selections, String testName, int sortIndex, Class<T> clazz) throws Exception {
        SessionNavigationBean sessionNavigationBean = new SessionNavigationBean();
        sessionNavigationBean.setDomain(testName + "domain");
        sessionNavigationBean.setAppId(testName + "appid");
        sessionNavigationBean.setUsername(testName + "username");
        sessionNavigationBean.setInstallReference("archives/" + testName + ".ccz");
        sessionNavigationBean.setSelections(selections);
        sessionNavigationBean.setSortIndex(sortIndex);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_MENU_NAVIGATION,
                sessionNavigationBean,
                clazz);
    }

    <T> T sessionNavigate(String[] selections, String testName, String locale, Class<T> clazz, String restoreAs) throws Exception {
        SessionNavigationBean sessionNavigationBean = new SessionNavigationBean();
        sessionNavigationBean.setDomain(testName + "domain");
        sessionNavigationBean.setAppId(testName + "appid");
        sessionNavigationBean.setUsername(testName + "username");
        sessionNavigationBean.setInstallReference("archives/" + testName + ".ccz");
        sessionNavigationBean.setSelections(selections);
        sessionNavigationBean.setRestoreAs(restoreAs);
        if (locale != null && !"".equals(locale.trim())) {
            sessionNavigationBean.setLocale(locale);
        }
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_MENU_NAVIGATION,
                sessionNavigationBean,
                clazz);
    }

    <T> T sessionNavigateWithQuery(String[] selections,
                                   String testName,
                                   Hashtable<String, String> queryDictionary,
                                   Class<T> clazz) throws Exception {
        SessionNavigationBean sessionNavigationBean = new SessionNavigationBean();
        sessionNavigationBean.setSelections(selections);
        sessionNavigationBean.setDomain(testName + "domain");
        sessionNavigationBean.setAppId(testName + "appid");
        sessionNavigationBean.setUsername(testName + "username");
        sessionNavigationBean.setInstallReference("archives/" + testName + ".ccz");
        sessionNavigationBean.setQueryDictionary(queryDictionary);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_MENU_NAVIGATION,
                sessionNavigationBean,
                clazz);
    }

    protected CommandListResponseBean doInstall(String requestPath) throws Exception {
        InstallRequestBean installRequestBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), InstallRequestBean.class);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_INSTALL,
                installRequestBean,
                CommandListResponseBean.class);
    }

    CommandListResponseBean doUpdate(String requestPath) throws Exception {
        InstallRequestBean installRequestBean = mapper.readValue
                (FileUtils.getFile(this.getClass(), requestPath), InstallRequestBean.class);
        return generateMockQuery(ControllerType.MENU,
                RequestType.POST,
                Constants.URL_UPDATE,
                installRequestBean,
                CommandListResponseBean.class);
    }

    public enum RequestType {
        POST, GET
    }

    public enum ControllerType {
        FORM, MENU, UTIL, DEBUGGER,
    }

    private <T> T generateMockQuery(ControllerType controllerType,
                                    RequestType requestType,
                                    String urlPath,
                                    Object bean,
                                    Class<T> clazz) throws Exception {
        MockMvc controller = null;
        ResultActions result = null;

        if (bean instanceof AuthenticatedRequestBean) {
            restoreFactoryMock.getSQLiteDB().closeConnection();
            restoreFactoryMock.configure((AuthenticatedRequestBean) bean, new DjangoAuth("derp"), false);
        }

        if (bean instanceof InstallRequestBean) {
            storageFactoryMock.getSQLiteDB().closeConnection();
            storageFactoryMock.configure((InstallRequestBean) bean);
        }

        if (bean instanceof SessionRequestBean) {
            storageFactoryMock.getSQLiteDB().closeConnection();
            storageFactoryMock.configure(((SessionRequestBean) bean).getSessionId());
        }

        if (!(bean instanceof String)) {
            bean = mapper.writeValueAsString(bean);
        }
        switch (controllerType) {
            case FORM:
                controller = mockFormController;
                break;
            case MENU:
                controller = mockMenuController;
                break;
            case UTIL:
                controller = mockUtilController;
                break;
            case DEBUGGER:
                controller = mockDebuggerController;
                break;
        }
        switch (requestType) {
            case POST:
                result = controller.perform(
                        post(urlPrepend(urlPath))
                                .contentType(MediaType.APPLICATION_JSON)
                                .cookie(new Cookie(Constants.POSTGRES_DJANGO_SESSION_ID, "derp"))
                                .content((String) bean));
                break;

            case GET:
                result = controller.perform(
                        get(urlPrepend(urlPath))
                                .contentType(MediaType.APPLICATION_JSON)
                                .cookie(new Cookie(Constants.POSTGRES_DJANGO_SESSION_ID, "derp"))
                                .content((String) bean));
                break;
        }
        restoreFactoryMock.getSQLiteDB().closeConnection();
        storageFactoryMock.getSQLiteDB().closeConnection();
        return mapper.readValue(
                result.andReturn().getResponse().getContentAsString(),
                clazz
        );
    }

    public class MockTimezoneProvider extends TimezoneProvider {

        private int offsetMillis;

        public void setOffset(int offset) {
            this.offsetMillis = offset;
        }

        @Override
        public int getTimezoneOffsetMillis() {
            return offsetMillis;
        }
    }

}
