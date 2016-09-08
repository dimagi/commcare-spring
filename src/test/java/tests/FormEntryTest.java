package tests;

import beans.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import util.Constants;
import utils.FileUtils;
import utils.TestContext;

import java.util.ArrayList;
import java.util.Arrays;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContext.class)
public class FormEntryTest extends BaseTestClass{

    //Integration test of form entry functions
    @Test
    public void testFormEntry() throws Exception {

        serializableFormSession.setRestoreXml(FileUtils.getFile(this.getClass(), "test_restore.xml"));

        NewFormSessionResponse newSessionResponse = startNewSession("requests/new_form/new_form_2.json", "xforms/question_types.xml");

        String sessionId = newSessionResponse.getSessionId();

        FormEntryResponseBean response = answerQuestionGetResult("1","William Pride", sessionId, 1);
        response = answerQuestionGetResult("2","345", sessionId, 2);
        response = answerQuestionGetResult("3","2.54", sessionId, 3);
        response = answerQuestionGetResult("4","1970-10-23", sessionId, 4);
        response = answerQuestionGetResult("6", "12:30:30", sessionId, 5);
        response = answerQuestionGetResult("7", "ben rudolph", sessionId, 6);
        response = answerQuestionGetResult("8","123456789", sessionId, 7);
        response = answerQuestionGetResult("10", "2",sessionId, 8);
        response = answerQuestionGetResult("11", "1 2 3", sessionId, 9);

        QuestionBean[] tree = response.getTree();

        QuestionBean textBean = tree[1];
        assert textBean.getAnswer().equals("William Pride");

        QuestionBean intBean = tree[2];
        assert intBean.getAnswer().equals(345);

        QuestionBean decimalBean = tree[3];
        assert decimalBean.getAnswer().equals(2.54);

        QuestionBean dateBean = tree[4];
        assert dateBean.getAnswer().equals("1970-10-23");

        QuestionBean multiSelectQuestion = tree[11];
        assert(multiSelectQuestion.getAnswer() instanceof ArrayList);
        ArrayList<Integer> answer = (ArrayList<Integer>) multiSelectQuestion.getAnswer();
        assert(answer.size() == 3);
        assert answer.get(0).equals(1);

        response = answerQuestionGetResult("12", "1", sessionId, 10);
        tree = response.getTree();
        multiSelectQuestion = tree[11];
        assert(multiSelectQuestion.getAnswer() instanceof ArrayList);
        answer = (ArrayList<Integer>) multiSelectQuestion.getAnswer();
        assert(answer.size() == 3);
        assert answer.get(0).equals(1);

        response = answerQuestionGetResult("17", "[13.803252972154226, 7.723388671875]", sessionId, 11);
        QuestionBean geoBean = response.getTree()[17];
        assert geoBean.getAnswer() instanceof  ArrayList;
        ArrayList<Double> geoCoordinates = (ArrayList<Double>) geoBean.getAnswer();
        Double latitude = geoCoordinates.get(0);
        assert latitude.equals(13.803252972154226);
        Double longitude = geoCoordinates.get(1);
        assert longitude.equals(7.723388671875);


        //Test Current Session
        FormEntryResponseBean formEntryResponseBean = getCurrent(sessionId);

        //Test Get Instance
        GetInstanceResponseBean getInstanceResponseBean = getInstance(sessionId);

        //Test Evaluate XPath
        EvaluateXPathResponseBean evaluateXPathResponseBean = evaluateXPath(sessionId, "/data/q_text");
        assert evaluateXPathResponseBean.getStatus().equals(Constants.ANSWER_RESPONSE_STATUS_POSITIVE);
        assert evaluateXPathResponseBean.getOutput().equals("William Pride");

        evaluateXPathResponseBean = evaluateXPath(sessionId, "/data/broken");
        assert evaluateXPathResponseBean.getStatus().equals(Constants.ANSWER_RESPONSE_STATUS_NEGATIVE);

        //Test Submission
        SubmitResponseBean submitResponseBean = submitForm("requests/submit/submit_request.json", sessionId);
    }


    //Integration test of form entry functions
    @Test
    public void testFormEntry2() throws Exception {

        serializableFormSession.setRestoreXml(FileUtils.getFile(this.getClass(), "test_restore.xml"));

        NewFormSessionResponse newSessionResponse = startNewSession("requests/new_form/new_form_2.json", "xforms/question_types_2.xml");

        String sessionId = newSessionResponse.getSessionId();

        answerQuestionGetResult("1","William Pride", sessionId, 1);

        answerQuestionGetResult("8,1","1", sessionId, 2);
        FormEntryResponseBean response = answerQuestionGetResult("8,2","2", sessionId, 3);

        QuestionBean questionBean = response.getTree()[8];
        QuestionBean[] children = questionBean.getChildren();

        assert children.length == 4;

        QuestionBean red = children[1];
        QuestionBean blue = children[2];

        assert red.getAnswer().equals(1);
        assert blue.getAnswer().equals(2);

        response = answerQuestionGetResult("8,3","2", sessionId, 4);

        questionBean = response.getTree()[8];
        children = questionBean.getChildren();

        red = children[1];
        blue = children[2];
        QuestionBean green = children[3];

        assert red.getAnswer().equals(1);
        assert blue.getAnswer().equals(2);
        assert green.getAnswer().equals(2);

    }
}