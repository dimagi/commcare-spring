package org.commcare.formplayer.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.formplayer.annotations.AppInstall;
import org.commcare.formplayer.annotations.UserLock;
import org.commcare.formplayer.annotations.UserRestore;
import org.commcare.formplayer.beans.SessionNavigationBean;
import org.commcare.formplayer.beans.menus.BaseResponseBean;
import org.commcare.formplayer.beans.menus.EntityDetailListResponse;
import org.commcare.formplayer.beans.menus.EntityDetailResponse;
import org.commcare.formplayer.beans.menus.LocationRelevantResponseBean;
import org.commcare.formplayer.services.CategoryTimingHelper;
import org.commcare.formplayer.services.FormplayerStorageFactory;
import org.commcare.formplayer.session.MenuSession;
import org.commcare.formplayer.util.Constants;
import org.commcare.util.screen.EntityScreen;
import org.commcare.util.screen.EntityScreenContext;
import org.commcare.util.screen.Screen;
import org.javarosa.core.model.instance.TreeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * Controller (API endpoint) containing all session navigation functionality.
 * This includes module, form, case, and session (incomplete form) selection.
 */
@RestController
@EnableAutoConfiguration
public class MenuController extends AbstractBaseController {

    @Autowired
    private CategoryTimingHelper categoryTimingHelper;

    @Autowired
    protected FormplayerStorageFactory storageFactory;

    private final Log log = LogFactory.getLog(MenuController.class);

    @RequestMapping(value = Constants.URL_GET_DETAILS, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public EntityDetailListResponse getDetails(@RequestBody SessionNavigationBean sessionNavigationBean,
                                               @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken,
                                               HttpServletRequest request) throws Exception {
        MenuSession menuSession = getMenuSessionFromBean(sessionNavigationBean);
        boolean isFuzzySearch = storageFactory.getPropertyManager().isFuzzySearchEnabled();
        if (sessionNavigationBean.getIsPersistent()) {
            EntityScreenContext entityScreenContext = new EntityScreenContext(sessionNavigationBean.getOffset(),
                    sessionNavigationBean.getSearchText(),
                    sessionNavigationBean.getSortIndex(),
                    sessionNavigationBean.getCasesPerPage(),
                    sessionNavigationBean.getSelectedValues(),
                    null,
                    isFuzzySearch);
            BaseResponseBean baseResponseBean = runnerService.advanceSessionWithSelections(menuSession,
                    sessionNavigationBean.getSelections(),
                    sessionNavigationBean.getQueryData(),
                    entityScreenContext,
                    null
            );
            logNotification(baseResponseBean.getNotification(),request);
            // See if we have a persistent case tile to expand
            EntityDetailListResponse detail = runnerService.getInlineDetail(menuSession, storageFactory.getPropertyManager().isFuzzySearchEnabled());
            if (detail == null) {
                throw new RuntimeException("Could not get inline details");
            }
            return setLocationNeeds(detail, menuSession);
        }

        String[] selections = sessionNavigationBean.getSelections();
        String[] commitSelections = new String[selections.length - 1];
        String detailSelection = selections[selections.length - 1];
        System.arraycopy(selections, 0, commitSelections, 0, selections.length - 1);

        EntityScreenContext entityScreenContext = new EntityScreenContext(sessionNavigationBean.getOffset(),
                sessionNavigationBean.getSearchText(),
                sessionNavigationBean.getSortIndex(),
                sessionNavigationBean.getCasesPerPage(),
                sessionNavigationBean.getSelectedValues(),
                detailSelection,
                isFuzzySearch);
        BaseResponseBean baseResponseBean = runnerService.advanceSessionWithSelections(
                menuSession,
                commitSelections,
                sessionNavigationBean.getQueryData(),
                entityScreenContext,
                null
        );
        logNotification(baseResponseBean.getNotification(),request);

        Screen currentScreen = menuSession.getNextScreen(true, entityScreenContext);

        if (!(currentScreen instanceof EntityScreen)) {
            // See if we have a persistent case tile to expand
            EntityDetailResponse detail = runnerService.getPersistentDetail(menuSession, storageFactory.getPropertyManager().isFuzzySearchEnabled());
            if (detail == null) {
                throw new RuntimeException("Tried to get details while not on a case list.");
            }
            return setLocationNeeds(new EntityDetailListResponse(detail), menuSession);
        }
        EntityScreen entityScreen = (EntityScreen)currentScreen;
        TreeReference reference = entityScreen.resolveTreeReference(detailSelection);

        if (reference == null) {
            throw new RuntimeException("Could not find case with ID " + detailSelection);
        }

        restoreFactory.cacheSessionSelections(selections);
        return setLocationNeeds(
                new EntityDetailListResponse(entityScreen,
                        menuSession.getEvalContextWithHereFuncHandler(),
                        reference,
                        storageFactory.getPropertyManager().isFuzzySearchEnabled()),
                menuSession
        );
    }

    /**
     * Make a a series of menu selections (as above, but can have multiple)
     *
     * @param sessionNavigationBean Give an installation code or path and a set of session selections
     * @param authToken             The Django session id auth token
     * @return A MenuBean or a NewFormResponse
     */
    @RequestMapping(value = {Constants.URL_MENU_NAVIGATION, Constants.URL_INITIAL_MENU_NAVIGATION}, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public BaseResponseBean navigateSessionWithAuth(@RequestBody SessionNavigationBean sessionNavigationBean,
                                                    @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken,
                                                    HttpServletRequest request) throws Exception {
        String[] selections = sessionNavigationBean.getSelections();
        MenuSession menuSession;
        menuSession = getMenuSessionFromBean(sessionNavigationBean);
        EntityScreenContext entityScreenContext = new EntityScreenContext(sessionNavigationBean.getOffset(),
                sessionNavigationBean.getSearchText(),
                sessionNavigationBean.getSortIndex(),
                sessionNavigationBean.getCasesPerPage(),
                sessionNavigationBean.getSelectedValues(),
                null,
                storageFactory.getPropertyManager().isFuzzySearchEnabled());
        BaseResponseBean response = runnerService.advanceSessionWithSelections(
                menuSession,
                selections,
                sessionNavigationBean.getQueryData(),
                entityScreenContext,
                sessionNavigationBean.getFormSessionId()
        );
        logNotification(response.getNotification(), request);
        return setLocationNeeds(response, menuSession);
    }

    private static <T extends LocationRelevantResponseBean> T setLocationNeeds(T responseBean, MenuSession menuSession) {
        responseBean.setShouldRequestLocation(menuSession.locationRequestNeeded());
        responseBean.setShouldWatchLocation(menuSession.hereFunctionEvaluated());
        return responseBean;
    }

    @RequestMapping(value = Constants.URL_GET_ENDPOINT, method = RequestMethod.POST)
    @UserLock
    @UserRestore
    @AppInstall
    public BaseResponseBean navigateToEndpoint(@RequestBody SessionNavigationBean sessionNavigationBean,
                                                    @CookieValue(Constants.POSTGRES_DJANGO_SESSION_ID) String authToken,
                                                    HttpServletRequest request) throws Exception {
        // Apps using aggressive syncs are likely to hit a sync whenever using endpoint-based navigation,
        // since they use it to jump between different sandboxes. Turn it off.
        restoreFactory.setPermitAggressiveSyncs(false);

        MenuSession menuSession = getMenuSessionFromBean(sessionNavigationBean);
        BaseResponseBean response = runnerService.advanceSessionWithEndpoint(menuSession,
                sessionNavigationBean.getEndpointId(),
                sessionNavigationBean.getEndpointArgs());
        logNotification(response.getNotification(), request);
        return setLocationNeeds(response, menuSession);
    }
}
