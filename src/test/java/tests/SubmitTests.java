package tests;

import beans.NewFormResponse;
import beans.SubmitResponseBean;
import org.commcare.cases.model.Case;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import sandbox.SqlStorage;
import sandbox.UserSqlSandbox;
import utils.TestContext;

import static org.mockito.Matchers.anyString;

/**
 * Regression tests for submission behaviors
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContext.class)
public class SubmitTests extends BaseTestClass {

    @Test
    public void testCaseCreateFails() throws Exception {
        // Start new session and submit create case form
        NewFormResponse newSessionResponse = startNewForm("requests/new_form/new_form_3.json",
                "xforms/cases/create_case.xml");

        UserSqlSandbox sandbox = restoreFactoryMock.getSqlSandbox();
        SqlStorage<Case> caseStorage =  sandbox.getCaseStorage();
        assert(caseStorage.getNumRecords() == 15);

        Mockito.doReturn(new ResponseEntity<>(HttpStatus.BAD_REQUEST))
                .when(submitServiceMock).submitForm(anyString(), anyString());
        // Assert that FormSession is not deleted
        Mockito.doThrow(new RuntimeException())
                .when(formSessionRepoMock).delete(anyString());

        SubmitResponseBean submitResponseBean = submitForm("requests/submit/submit_request_case.json", "derp");
        assert submitResponseBean.getStatus().equals("error");
        // Assert that case is not created
        assert(caseStorage.getNumRecords()== 15);
    }

}
