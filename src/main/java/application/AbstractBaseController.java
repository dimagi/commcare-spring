package application;

import aspects.LockAspect;
import beans.InstallRequestBean;
import beans.SessionNavigationBean;
import beans.exceptions.ExceptionResponseBean;
import beans.exceptions.HTMLExceptionResponseBean;
import beans.exceptions.RetryExceptionResponseBean;
import com.timgroup.statsd.StatsDClient;
import exceptions.*;
import io.sentry.event.Event;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.core.process.CommCareInstanceInitializer;
import org.commcare.modern.models.RecordTooLargeException;
import org.commcare.util.screen.CommCareSessionException;
import org.javarosa.core.model.actions.FormSendCalloutHandler;
import org.javarosa.xml.util.InvalidStructureException;
import org.javarosa.xpath.XPathException;
import org.javarosa.xpath.XPathTypeMismatchException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.client.HttpClientErrorException;
import repo.FormSessionRepo;
import repo.MenuSessionRepo;
import repo.impl.PostgresUserRepo;
import services.*;
import session.MenuSession;
import util.Constants;
import util.FormplayerHttpRequest;
import util.FormplayerSentry;

/**
 * Base Controller class containing exception handling logic and
 * autowired beans used in both MenuController and FormController
 */
public abstract class AbstractBaseController {

    @Value("${commcarehq.host}")
    protected String host;

    @Autowired
    private QueryRequester queryRequester;

    @Autowired
    private SyncRequester syncRequester;

    @Autowired
    protected FormSessionRepo formSessionRepo;

    @Autowired
    protected MenuSessionRepo menuSessionRepo;

    @Autowired
    protected InstallService installService;

    @Autowired
    protected RestoreFactory restoreFactory;

    @Autowired
    protected NewFormResponseFactory newFormResponseFactory;

    @Autowired
    protected FormplayerStorageFactory storageFactory;

    @Autowired
    protected StatsDClient datadogStatsDClient;

    @Autowired
    protected FormplayerSentry raven;

    @Autowired
    protected FormSendCalloutHandler formSendCalloutHandler;

    @Autowired
    PostgresUserRepo postgresUserRepo;

    @Value("${commcarehq.environment}")
    private String hqEnvironment;

    @Autowired
    protected MenuSessionFactory menuSessionFactory;

    @Autowired
    protected MenuSessionRunnerService runnerService;

    private final Log log = LogFactory.getLog(AbstractBaseController.class);

    /**
     * Catch all the exceptions that we *do not* want emailed here
     */
    @ExceptionHandler({ApplicationConfigException.class,
            XPathException.class,
            CommCareInstanceInitializer.FixtureInitializationException.class,
            CommCareSessionException.class,
            FormNotFoundException.class,
            RecordTooLargeException.class,
            InvalidStructureException.class,
            UnresolvedResourceRuntimeException.class})
    @ResponseBody
    public ExceptionResponseBean handleApplicationError(FormplayerHttpRequest request, Exception exception) {
        log.error("Request: " + request.getRequestURL() + " raised " + exception);
        incrementDatadogCounter(Constants.DATADOG_ERRORS_APP_CONFIG, request);
        raven.sendRavenException(exception, Event.Level.INFO);
        return getPrettyExceptionResponse(exception, request);
    }

    private ExceptionResponseBean getPrettyExceptionResponse(Exception exception, FormplayerHttpRequest request) {
        String message = exception.getMessage();
        if (exception instanceof XPathTypeMismatchException && message.contains("instance(groups)")) {
            message = "The case sharing settings for your user are incorrect. " +
                    "This user must be in exactly one case sharing group. " +
                    "Please contact your supervisor.";
        }
        return new ExceptionResponseBean(message, request.getRequestURL().toString());
    }

    /**
     * Handles exceptions thrown when making external requests, usually to CommCareHQ.
     */
    @ExceptionHandler({HttpClientErrorException.class})
    @ResponseBody
    public ExceptionResponseBean handleHttpRequestError(FormplayerHttpRequest req, HttpClientErrorException exception) {
        incrementDatadogCounter(Constants.DATADOG_ERRORS_EXTERNAL_REQUEST, req);
        log.error(String.format("Exception %s making external request %s.", exception, req));
        return new ExceptionResponseBean(exception.getResponseBodyAsString(), req.getRequestURL().toString());
    }

    @ExceptionHandler({AsyncRetryException.class})
    @ResponseStatus(value = HttpStatus.ACCEPTED)
    @ResponseBody
    public RetryExceptionResponseBean handleAsyncRetryException(FormplayerHttpRequest req, AsyncRetryException exception) {
        return new RetryExceptionResponseBean(
                exception.getMessage(),
                req.getRequestURL().toString(),
                exception.getDone(),
                exception.getTotal(),
                exception.getRetryAfter()
        );
    }
    /**
     * Catch exceptions that have formatted HTML errors
     */
    @ExceptionHandler({FormattedApplicationConfigException.class})
    @ResponseBody
    public HTMLExceptionResponseBean handleFormattedApplicationError(FormplayerHttpRequest req, Exception exception) {
        log.error("Request: " + req.getRequestURL() + " raised " + exception);
        incrementDatadogCounter(Constants.DATADOG_ERRORS_APP_CONFIG, req);

        return new HTMLExceptionResponseBean(exception.getMessage(), req.getRequestURL().toString());
    }

    @ExceptionHandler({LockAspect.LockError.class})
    @ResponseBody
    @ResponseStatus(HttpStatus.LOCKED)
    public ExceptionResponseBean handleLockError(FormplayerHttpRequest req, Exception exception) {
        return new ExceptionResponseBean("User lock timed out", req.getRequestURL().toString());
    }

    @ExceptionHandler({InterruptedRuntimeException.class})
    @ResponseBody
    public ExceptionResponseBean handleInterruptException(FormplayerHttpRequest req, Exception exception) {
        return new ExceptionResponseBean("An issue prevented us from processing your previous action, please try again",
                req.getRequestURL().toString());
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public ExceptionResponseBean handleError(FormplayerHttpRequest req, Exception exception) {
        log.error("Request: " + req.getRequestURL() + " raised " + exception);
        incrementDatadogCounter(Constants.DATADOG_ERRORS_CRASH, req);
        exception.printStackTrace();
        raven.sendRavenException(exception);
        if (exception instanceof ClientAbortException) {
            // We can't actually return anything since the client has bailed. To avoid errors return null
            // https://mtyurt.net/2016/04/18/spring-how-to-handle-ioexception-broken-pipe/
            log.error("Client Aborted! Returning null");
            return null;
        }
        return new ExceptionResponseBean(exception.getMessage(), req.getRequestURL().toString());
    }

    private void incrementDatadogCounter(String metric, FormplayerHttpRequest req) {
        String user = "unknown";
        String domain = "unknown";
        if (req.getUserDetails() != null) {
            user = req.getUserDetails().getUsername();
        }
        if (req.getDomain() != null) {
            domain = req.getDomain();
        }
        datadogStatsDClient.increment(
                metric,
                "domain:" + domain,
                "user:" + user,
                "request:" + req.getRequestURI()
        );
    }

    protected MenuSession getMenuSessionFromBean(SessionNavigationBean sessionNavigationBean) throws Exception {
        return performInstall(sessionNavigationBean);
    }

    protected MenuSession performInstall(InstallRequestBean bean) throws Exception {
        if ((bean.getAppId() == null || "".equals(bean.getAppId())) &&
                bean.getInstallReference() == null || "".equals(bean.getInstallReference())) {
            throw new RuntimeException("Either app_id or installReference must be non-null.");
        }

        return menuSessionFactory.buildSession(
                bean.getUsername(),
                bean.getDomain(),
                bean.getAppId(),
                bean.getInstallReference(),
                bean.getLocale(),
                bean.getOneQuestionPerScreen(),
                bean.getRestoreAs(),
                bean.getPreview()
        );
    }

}
