package application;

import annotations.UserLock;
import auth.DjangoAuth;
import auth.HqAuth;
import beans.*;
import beans.menus.ErrorBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import objects.SerializableFormSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.api.json.JsonActionUtils;
import org.commcare.api.process.FormRecordProcessorHelper;
import org.commcare.api.util.ApiConstants;
import org.javarosa.form.api.FormEntryController;
import org.javarosa.form.api.FormEntryModel;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import repo.SerializableMenuSession;
import services.NewFormResponseFactory;
import services.SubmitService;
import services.XFormService;
import session.FormSession;
import session.MenuSession;
import util.Constants;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Controller class (API endpoint) containing all form entry logic. This includes
 * opening a new form, question answering, and form submission.
 */
@Api(value = "Form Controller", description = "Operations for navigating CommCare Forms")
@RestController
@EnableAutoConfiguration
public class FormController extends AbstractBaseController{

    @Autowired
    private XFormService xFormService;

    @Autowired
    private SubmitService submitService;

    @Autowired
    private NewFormResponseFactory newFormResponseFactory;

    @Value("${commcarehq.host}")
    private String host;

    private final Log log = LogFactory.getLog(FormController.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @ApiOperation(value = "Start a new form entry session")
    @RequestMapping(value = Constants.URL_NEW_SESSION , method = RequestMethod.POST)
    @UserLock
    public NewFormResponse newFormResponse(@RequestBody NewSessionRequestBean newSessionBean,
                                           @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        String postUrl = host + newSessionBean.getPostUrl();
        NewFormResponse newSessionResponse = newFormResponseFactory.getResponse(newSessionBean,
                postUrl,
                new DjangoAuth(authToken));
        return newSessionResponse;
    }

    @ApiOperation(value = "Open an incomplete form session")
    @RequestMapping(value = Constants.URL_INCOMPLETE_SESSION , method = RequestMethod.POST)
    @UserLock
    public NewFormResponse openIncompleteForm(@RequestBody IncompleteSessionRequestBean incompleteSessionRequestBean,
                                              @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        SerializableFormSession session = formSessionRepo.findOneWrapped(incompleteSessionRequestBean.getSessionId());
        NewFormResponse response = newFormResponseFactory.getResponse(session);
        return response;
    }

    @ApiOperation(value = "Delete an incomplete form session")
    @RequestMapping(value = Constants.URL_DELETE_INCOMPLETE_SESSION , method = RequestMethod.POST)
    @UserLock
    public NotificationMessageBean deleteIncompleteForm(
            @RequestBody IncompleteSessionRequestBean incompleteSessionRequestBean,
            @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        formSessionRepo.delete(incompleteSessionRequestBean.getSessionId());
        return new NotificationMessageBean("Successfully deleted incomplete form.", false);
    }


    @ApiOperation(value = "Answer the question at the given index")
    @RequestMapping(value = Constants.URL_ANSWER_QUESTION, method = RequestMethod.POST)
    @UserLock
    public FormEntryResponseBean answerQuestion(@RequestBody AnswerQuestionRequestBean answerQuestionBean) throws Exception {
        SerializableFormSession session = formSessionRepo.findOneWrapped(answerQuestionBean.getSessionId());
        FormSession formEntrySession = new FormSession(session);
        JSONObject resp = formEntrySession.answerQuestionToJSON(answerQuestionBean.getAnswer(),
                answerQuestionBean.getFormIndex());
        updateSession(formEntrySession, session);
        FormEntryResponseBean responseBean = mapper.readValue(resp.toString(), FormEntryResponseBean.class);
        responseBean.setTitle(formEntrySession.getTitle());
        responseBean.setSequenceId(formEntrySession.getSequenceId());
        responseBean.setInstanceXml(new InstanceXmlBean(formEntrySession));
        return responseBean;
    }

    @ApiOperation(value = "Get the current question (deprecated)")
    @RequestMapping(value = Constants.URL_CURRENT, method = RequestMethod.GET)
    @ResponseBody
    @UserLock
    public FormEntryResponseBean getCurrent(@RequestBody CurrentRequestBean currentRequestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(currentRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        JSONObject resp = JsonActionUtils.getCurrentJson(formEntrySession.getFormEntryController(), formEntrySession.getFormEntryModel());
        FormEntryResponseBean responseBean = mapper.readValue(resp.toString(), FormEntryResponseBean.class);
        return responseBean;
    }

    @ApiOperation(value = "Submit the current form")
    @RequestMapping(value = Constants.URL_SUBMIT_FORM, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public SubmitResponseBean submitForm(@RequestBody SubmitRequestBean submitRequestBean,
                                             @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(submitRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        SubmitResponseBean submitResponseBean;

        if (serializableFormSession.getOneQuestionPerScreen()) {
            // todo separate workflow for one question per screen
            submitResponseBean = new SubmitResponseBean(Constants.SYNC_RESPONSE_STATUS_POSITIVE);
        } else {
            submitResponseBean = validateSubmitAnswers(formEntrySession.getFormEntryController(),
                formEntrySession.getFormEntryModel(),
                submitRequestBean.getAnswers());
        }

        if (!submitResponseBean.getStatus().equals(Constants.SYNC_RESPONSE_STATUS_POSITIVE)
                || !submitRequestBean.isPrevalidated()) {
            submitResponseBean.setStatus(Constants.ANSWER_RESPONSE_STATUS_NEGATIVE);
        } else {
            FormRecordProcessorHelper.processXML(formEntrySession.getSandbox(), formEntrySession.submitGetXml());

            ResponseEntity<String> submitResponse =
                    submitService.submitForm(formEntrySession.getInstanceXml(),
                            formEntrySession.getPostUrl(),
                            new DjangoAuth(authToken));

            if (!submitResponse.getStatusCode().is2xxSuccessful()) {
                submitResponseBean.setStatus("error");
                submitResponseBean.setNotification(new NotificationMessageBean(
                        "Form submission failed with error response" + submitResponse, true));
                log.info("Submit response bean: " + submitResponseBean);
                return submitResponseBean;
            }

            if (formEntrySession.getMenuSessionId() != null &&
                    !("").equals(formEntrySession.getMenuSessionId().trim())) {
                Object nav = doEndOfFormNav(menuSessionRepo.findOneWrapped(formEntrySession.getMenuSessionId()), new DjangoAuth(authToken));
                if (nav != null) {
                    submitResponseBean.setNextScreen(nav);
                }
            }
            formSessionRepo.delete(submitRequestBean.getSessionId());
        }
        return submitResponseBean;
    }

    private Object doEndOfFormNav(SerializableMenuSession serializedSession, HqAuth auth) throws Exception {
        log.info("End of form navigation with serialized menu session: " + serializedSession);
        MenuSession menuSession = new MenuSession(serializedSession, installService, restoreFactory, auth, host);
        return resolveFormGetNext(menuSession);
    }

    /**
     * Iterate over all answers and attempt to save them to check for validity.
     * Submit the complete XML instance to HQ if valid.
     */
    private SubmitResponseBean validateSubmitAnswers(FormEntryController formEntryController,
                                       FormEntryModel formEntryModel,
                                       Map<String, Object> answers) {
        SubmitResponseBean submitResponseBean = new SubmitResponseBean(Constants.SYNC_RESPONSE_STATUS_POSITIVE);
        HashMap<String, ErrorBean> errors = new HashMap<>();
        for(String key: answers.keySet()){
            int questionType = JsonActionUtils.getQuestionType(formEntryModel, key, formEntryModel.getForm());
            if(!(questionType == FormEntryController.EVENT_QUESTION)){
                continue;
            }
            String answer = answers.get(key) == null ? null : answers.get(key).toString();
            JSONObject answerResult =
                    JsonActionUtils.questionAnswerToJson(formEntryController,
                            formEntryModel,
                            answer,
                            key);
            if(!answerResult.get(ApiConstants.RESPONSE_STATUS_KEY).equals(Constants.ANSWER_RESPONSE_STATUS_POSITIVE)) {
                submitResponseBean.setStatus(Constants.ANSWER_RESPONSE_STATUS_NEGATIVE);
                ErrorBean error = new ErrorBean();
                error.setStatus(answerResult.get(ApiConstants.RESPONSE_STATUS_KEY).toString());
                error.setType(answerResult.getString(ApiConstants.ERROR_TYPE_KEY));
                errors.put(key, error);
            }
        }
        submitResponseBean.setErrors(errors);
        return submitResponseBean;
    }

    @ApiOperation(value = "Get the current instance XML")
    @RequestMapping(value = Constants.URL_GET_INSTANCE, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public InstanceXmlBean getInstance(@RequestBody GetInstanceRequestBean getInstanceRequestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(getInstanceRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        InstanceXmlBean instanceXmlBean = new InstanceXmlBean(formEntrySession);
        return instanceXmlBean;
    }

    @ApiOperation(value = "Evaluate the given XPath under the current context")
    @RequestMapping(value = Constants.URL_EVALUATE_XPATH, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public EvaluateXPathResponseBean evaluateXpath(@RequestBody EvaluateXPathRequestBean evaluateXPathRequestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(evaluateXPathRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        EvaluateXPathResponseBean evaluateXPathResponseBean =
                new EvaluateXPathResponseBean(formEntrySession, evaluateXPathRequestBean.getXpath());
        return evaluateXPathResponseBean;
    }

    @ApiOperation(value = "Expand the repeat at the given index")
    @RequestMapping(value = Constants.URL_NEW_REPEAT, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public FormEntryResponseBean newRepeat(@RequestBody RepeatRequestBean newRepeatRequestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(newRepeatRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        JSONObject response = JsonActionUtils.descendRepeatToJson(formEntrySession.getFormEntryController(),
                formEntrySession.getFormEntryModel(),
                newRepeatRequestBean.getRepeatIndex());
        updateSession(formEntrySession, serializableFormSession);
        FormEntryResponseBean responseBean = mapper.readValue(response.toString(), FormEntryResponseBean.class);
        responseBean.setTitle(formEntrySession.getTitle());
        responseBean.setInstanceXml(new InstanceXmlBean(formEntrySession));
        log.info("New response: " + responseBean);
        return responseBean;
    }

    @ApiOperation(value = "Delete the repeat at the given index")
    @RequestMapping(value = Constants.URL_DELETE_REPEAT, method = RequestMethod.POST)
    @ResponseBody
    public FormEntryResponseBean deleteRepeat(@RequestBody RepeatRequestBean deleteRepeatRequestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(deleteRepeatRequestBean.getSessionId());
        FormSession formEntrySession = new FormSession(serializableFormSession);
        JSONObject response = JsonActionUtils.deleteRepeatToJson(formEntrySession.getFormEntryController(),
                formEntrySession.getFormEntryModel(),
                deleteRepeatRequestBean.getRepeatIndex(), deleteRepeatRequestBean.getFormIndex());
        updateSession(formEntrySession, serializableFormSession);
        FormEntryResponseBean responseBean = mapper.readValue(response.toString(), FormEntryResponseBean.class);
        responseBean.setTitle(formEntrySession.getTitle());
        responseBean.setInstanceXml(new InstanceXmlBean(formEntrySession));
        return responseBean;
    }

    // Functions pertaining to One Question Per Screen (OQPS) mode

    @ApiOperation(value = "Get the question for the current index")
    @RequestMapping(value = Constants.URL_QUESTIONS_FOR_INDEX, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public FormEntryResponseBean jumpToIndex(@RequestBody JumpToIndexRequestBean requestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(requestBean.getSessionId());
        FormSession formSession = new FormSession(serializableFormSession);
        formSession.setCurrentIndex(requestBean.getFormIndex());
        JSONObject resp = JsonActionUtils.getCurrentJson(formSession.getFormEntryController(),
                formSession.getFormEntryModel(),
                formSession.getCurrentIndex());
        updateSession(formSession, serializableFormSession);
        FormEntryResponseBean responseBean = mapper.readValue(resp.toString(), FormEntryResponseBean.class);
        return responseBean;
    }

    @ApiOperation(value = "Get the questions for the next index in OQPS mode")
    @RequestMapping(value = Constants.URL_NEXT_INDEX, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public FormEntryNavigationResponseBean getNext(@RequestBody SessionRequestBean requestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(requestBean.getSessionId());
        FormSession formSession = new FormSession(serializableFormSession);
        formSession.stepToNextIndex();
        JSONObject resp = formSession.getNextJson();
        updateSession(formSession, serializableFormSession);
        FormEntryNavigationResponseBean responseBean = mapper.readValue(resp.toString(), FormEntryNavigationResponseBean.class);
        return responseBean;
    }

    @ApiOperation(value = "Get the questios for the previous index in OQPS mode")
    @RequestMapping(value = Constants.URL_PREV_INDEX, method = RequestMethod.POST)
    @ResponseBody
    @UserLock
    public FormEntryNavigationResponseBean getPrevious(@RequestBody SessionRequestBean requestBean) throws Exception {
        SerializableFormSession serializableFormSession = formSessionRepo.findOneWrapped(requestBean.getSessionId());
        FormSession formSession = new FormSession(serializableFormSession);
        formSession.stepToPreviousIndex();
        JSONObject resp = JsonActionUtils.getCurrentJson(formSession.getFormEntryController(),
                formSession.getFormEntryModel(),
                formSession.getCurrentIndex());
        updateSession(formSession, serializableFormSession);
        FormEntryNavigationResponseBean responseBean = mapper.readValue(resp.toString(), FormEntryNavigationResponseBean.class);
        responseBean.setCurrentIndex(formSession.getCurrentIndex());
        return responseBean;
    }


    private void updateSession(FormSession formEntrySession, SerializableFormSession serialSession) throws IOException {
        formEntrySession.setSequenceId(formEntrySession.getSequenceId() + 1);
        serialSession.setInstanceXml(formEntrySession.getInstanceXml());
        serialSession.setSequenceId(formEntrySession.getSequenceId());
        serialSession.setCurrentIndex(formEntrySession.getCurrentIndex());
        formSessionRepo.save(serialSession);
    }
}
