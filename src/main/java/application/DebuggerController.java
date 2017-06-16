package application;

import annotations.AppInstall;
import annotations.AppInstallFromSession;
import annotations.UserLock;
import annotations.UserRestore;
import auth.DjangoAuth;
import beans.EvaluateXPathMenuRequestBean;
import beans.EvaluateXPathRequestBean;
import beans.EvaluateXPathResponseBean;
import beans.SessionRequestBean;
import beans.debugger.DebuggerFormattedQuestionsResponseBean;
import beans.debugger.XPathQueryItem;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import objects.SerializableFormSession;
import org.javarosa.xpath.expr.FunctionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;
import repo.SerializableMenuSession;
import services.FormattedQuestionsService;
import session.FormSession;
import session.MenuSession;
import util.Constants;

import javax.annotation.Resource;
import java.util.List;

/**
 * Controller class for all routes pertaining to the CloudCare Debugger
 */
@Api(value = "Debugger Controller", description = "Operations involving the CloudCare Debugger")
@RestController
@EnableAutoConfiguration
public class DebuggerController extends AbstractBaseController {

    private int MAX_RECENT = 5;

    @Autowired
    private FormattedQuestionsService formattedQuestionsService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Resource(name="redisTemplate")
    private ListOperations<String, XPathQueryItem> listOperations;

    @ApiOperation(value = "Get formatted questions and instance xml")
    @RequestMapping(value = Constants.URL_DEBUGGER_FORMATTED_QUESTIONS, method = RequestMethod.POST)
    @UserRestore
    public DebuggerFormattedQuestionsResponseBean getFormattedQuesitons(
            @RequestBody SessionRequestBean debuggerRequest,
            @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(debuggerRequest.getSessionId());
        FormSession formSession = new FormSession(serializableFormSession, restoreFactory);
        SerializableMenuSession serializableMenuSession = menuSessionRepo.findOne(serializableFormSession.getMenuSessionId());
        FormattedQuestionsService.QuestionResponse response = formattedQuestionsService.getFormattedQuestions(
                debuggerRequest.getDomain(),
                serializableMenuSession.getAppId(),
                formSession.getXmlns(),
                formSession.getInstanceXml(),
                new DjangoAuth(authToken)
        );
        return new DebuggerFormattedQuestionsResponseBean(
                serializableMenuSession.getAppId(),
                formSession.getXmlns(),
                formSession.getInstanceXml(),
                response.getFormattedQuestions(),
                response.getQuestionList(),
                FunctionUtils.xPathFuncList(),
                formSession.getFormEntryModel().getForm().getEvaluationContext().getInstanceIds(),
                fetchRecentFormXPathQueries(debuggerRequest.getDomain(), debuggerRequest.getUsername())
        );
    }

    @ApiOperation(value = "Evaluate the given XPath under the current context")
    @RequestMapping(value = Constants.URL_EVALUATE_XPATH, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    @UserRestore
    public EvaluateXPathResponseBean evaluateXpath(@RequestBody EvaluateXPathRequestBean evaluateXPathRequestBean,
                                                   @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(evaluateXPathRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession, restoreFactory);
        EvaluateXPathResponseBean evaluateXPathResponseBean = new EvaluateXPathResponseBean(
                formEntrySession.getFormEntryModel().getForm().getEvaluationContext(),
                evaluateXPathRequestBean.getXpath()
        );

        cacheFormXPathQuery(
                evaluateXPathRequestBean.getDomain(),
                evaluateXPathRequestBean.getUsername(),
                evaluateXPathRequestBean.getXpath(),
                evaluateXPathResponseBean.getOutput(),
                evaluateXPathResponseBean.getStatus()
        );

        return evaluateXPathResponseBean;
    }

    @ApiOperation(value = "Evaluate the given XPath under the current context")
    @RequestMapping(value = Constants.URL_EVALUATE_MENU_XPATH, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    @UserRestore
    @AppInstallFromSession
    public EvaluateXPathResponseBean menuEvaluateXpath(@RequestBody EvaluateXPathMenuRequestBean evaluateXPathRequestBean,
                                                   @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        MenuSession menuSession = getMenuSession(
                evaluateXPathRequestBean.getDomain(),
                evaluateXPathRequestBean.getUsername(),
                evaluateXPathRequestBean.getMenuSessionId(),
                authToken
        );

        EvaluateXPathResponseBean evaluateXPathResponseBean = new EvaluateXPathResponseBean(
                menuSession.getSessionWrapper().getEvaluationContext(),
                evaluateXPathRequestBean.getXpath()
        );

        cacheMenuXPathQuery(
                evaluateXPathRequestBean.getDomain(),
                evaluateXPathRequestBean.getUsername(),
                evaluateXPathRequestBean.getXpath(),
                evaluateXPathResponseBean.getOutput(),
                evaluateXPathResponseBean.getStatus()
        );

        return evaluateXPathResponseBean;
    }

    private List<XPathQueryItem> fetchRecentFormXPathQueries(String domain, String username) {
        return fetchRecentXPathQueries("menu", domain, username);
    }

    private void cacheMenuXPathQuery(String domain, String username, String xpath, String output, String status) {
        cacheXPathQuery("menu", domain, username, xpath, output, status);
    }

    private void cacheFormXPathQuery(String domain, String username, String xpath, String output, String status) {
        cacheXPathQuery("form", domain, username, xpath, output, status);
    }

    private void cacheXPathQuery(String prefix, String domain, String username, String xpath, String output, String status) {
        XPathQueryItem queryItem = new XPathQueryItem(xpath, output, status);

        listOperations.leftPush(
                redisXPathKey(prefix, domain, username),
                queryItem
        );
    }

    private List<XPathQueryItem> fetchRecentXPathQueries(String prefix, String domain, String username) {
        listOperations.trim(redisXPathKey(prefix, domain, username), 0, MAX_RECENT);
        return listOperations.range(redisXPathKey(prefix, domain, username), 0, MAX_RECENT);
    }

    private String redisXPathKey(String prefix, String domain, String username) {
        return "debugger:xpath:" + prefix + ":" + domain + ":" + username;
    }
}
