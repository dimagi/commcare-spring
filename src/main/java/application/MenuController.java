package application;

import annotations.AppInstall;
import annotations.UserLock;
import annotations.UserRestore;
import beans.InstallRequestBean;
import beans.NewFormResponse;
import beans.NotificationMessage;
import beans.SessionNavigationBean;
import beans.menus.BaseResponseBean;
import beans.menus.EntityDetailListResponse;
import beans.menus.EntityDetailResponse;
import beans.menus.UpdateRequestBean;
import exceptions.FormNotFoundException;
import exceptions.MenuNotFoundException;
import hq.CaseAPIs;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.util.screen.CommCareSessionException;
import org.commcare.util.screen.EntityScreen;
import org.commcare.util.screen.Screen;
import org.javarosa.core.model.instance.TreeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import repo.SerializableMenuSession;
import screens.FormplayerQueryScreen;
import screens.FormplayerSyncScreen;
import services.CategoryTimingHelper;
import services.QueryRequester;
import services.SyncRequester;
import session.FormSession;
import session.MenuSession;
import sqlitedb.ApplicationDB;
import util.Constants;
import util.SimpleTimer;
import util.Timing;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Hashtable;

/**
 * Controller (API endpoint) containing all session navigation functionality.
 * This includes module, form, case, and session (incomplete form) selection.
 */
@Api(value = "Menu Controllers", description = "Operations for navigating CommCare Menus and Cases")
@RestController
@EnableAutoConfiguration
public class MenuController extends AbstractBaseController {

    @Autowired
    private QueryRequester queryRequester;

    @Autowired
    private SyncRequester syncRequester;

    @Autowired
    private CategoryTimingHelper categoryTimingHelper;

    private final Log log = LogFactory.getLog(MenuController.class);

    @ApiOperation(value = "Install the application at the given reference")
    @RequestMapping(value = Constants.URL_INSTALL, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public BaseResponseBean installRequest(@RequestBody InstallRequestBean installRequestBean,
                                           @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        return getNextMenu(performInstall(installRequestBean));
    }

    @ApiOperation(value = "Update the application at the given reference")
    @RequestMapping(value = Constants.URL_UPDATE, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public BaseResponseBean updateRequest(@RequestBody UpdateRequestBean updateRequestBean,
                                          @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        MenuSession updatedSession = performUpdate(updateRequestBean);
        if (updateRequestBean.getSessionId() != null) {
            // Try restoring the old session, fail gracefully.
            try {
                FormSession oldSession = new FormSession(formSessionRepo.findOneWrapped(updateRequestBean.getSessionId()),
                        restoreFactory,
                        formSendCalloutHandler);
                updatedSession.reloadSession(oldSession);
                return new NewFormResponse(oldSession);
            } catch (FormNotFoundException e) {
                log.info("FormSession with id " + updateRequestBean.getSessionId() + " not found, returning root");
            } catch (Exception e) {
                log.info("FormSession with id " + updateRequestBean.getSessionId()
                        + " failed to load with exception " + e);
            }
        }
        return getNextMenu(updatedSession);
    }

    @RequestMapping(value = Constants.URL_GET_DETAILS, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public EntityDetailListResponse getDetails(@RequestBody SessionNavigationBean sessionNavigationBean,
                                               @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        MenuSession menuSession = getMenuSessionFromBean(sessionNavigationBean);
        if (sessionNavigationBean.getIsPersistent()) {
            advanceSessionWithSelections(menuSession,
                    sessionNavigationBean.getSelections(),
                    null,
                    sessionNavigationBean.getQueryDictionary(),
                    sessionNavigationBean.getOffset(),
                    sessionNavigationBean.getSearchText(),
                    sessionNavigationBean.getSortIndex()
            );

            // See if we have a persistent case tile to expand
            EntityDetailListResponse detail = getInlineDetail(menuSession);
            if (detail == null) {
                throw new RuntimeException("Could not get inline details");
            }
            return detail;
        }

        String[] selections = sessionNavigationBean.getSelections();
        String[] commitSelections = new String[selections.length - 1];
        String detailSelection = selections[selections.length - 1];
        System.arraycopy(selections, 0, commitSelections, 0, selections.length - 1);

        advanceSessionWithSelections(
                menuSession,
                commitSelections,
                detailSelection,
                sessionNavigationBean.getQueryDictionary(),
                sessionNavigationBean.getOffset(),
                sessionNavigationBean.getSearchText(),
                sessionNavigationBean.getSortIndex()
        );
        Screen currentScreen = menuSession.getNextScreen();

        if (!(currentScreen instanceof EntityScreen)) {
            // See if we have a persistent case tile to expand
            EntityDetailResponse detail = getPersistentDetail(menuSession);
            if (detail == null) {
                throw new RuntimeException("Tried to get details while not on a case list.");
            }
            return new EntityDetailListResponse(detail);
        }
        EntityScreen entityScreen = (EntityScreen) currentScreen;
        TreeReference reference = entityScreen.resolveTreeReference(detailSelection);

        if (reference == null) {
            throw new RuntimeException("Could not find case with ID " + detailSelection);
        }

        return new EntityDetailListResponse(
                entityScreen,
                menuSession.getSessionWrapper().getEvaluationContext(),
                reference
        );
    }

    /**
     * Make a a series of menu selections (as above, but can have multiple)
     *
     * @param sessionNavigationBean Give an installation code or path and a set of session selections
     * @param authToken             The Django session id auth token
     * @return A MenuBean or a NewFormResponse
     * @throws Exception
     */
    @RequestMapping(value = {Constants.URL_MENU_NAVIGATION, Constants.URL_INITIAL_MENU_NAVIGATION}, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public BaseResponseBean navigateSessionWithAuth(@RequestBody SessionNavigationBean sessionNavigationBean,
                                                    @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken) throws Exception {
        String[] selections = sessionNavigationBean.getSelections();
        MenuSession menuSession;
        menuSession = getMenuSessionFromBean(sessionNavigationBean);
        BaseResponseBean response = advanceSessionWithSelections(
                menuSession,
                selections,
                null,
                sessionNavigationBean.getQueryDictionary(),
                sessionNavigationBean.getOffset(),
                sessionNavigationBean.getSearchText(),
                sessionNavigationBean.getSortIndex()
        );
        return response;
    }

    private MenuSession performUpdate(UpdateRequestBean updateRequestBean) throws Exception {
        MenuSession currentSession = performInstall(updateRequestBean);
        currentSession.updateApp(updateRequestBean.getUpdateMode());
        return currentSession;
    }
}
