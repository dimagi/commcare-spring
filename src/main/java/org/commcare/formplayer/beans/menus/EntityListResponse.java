package org.commcare.formplayer.beans.menus;

import org.commcare.cases.entity.Entity;
import org.commcare.cases.entity.EntitySortNotificationInterface;
import org.commcare.cases.entity.EntitySorter;
import org.commcare.cases.entity.NodeEntityFactory;
import org.commcare.core.graph.model.GraphData;
import org.commcare.core.graph.util.GraphException;
import org.commcare.formplayer.exceptions.ApplicationConfigException;
import org.commcare.formplayer.util.EntityStringFilterer;
import org.commcare.formplayer.util.FormplayerGraphUtil;
import org.commcare.modern.session.SessionWrapper;
import org.commcare.modern.util.Pair;
import org.commcare.suite.model.Action;
import org.commcare.suite.model.Detail;
import org.commcare.suite.model.DetailField;
import org.commcare.suite.model.EntityDatum;
import org.commcare.suite.model.Style;
import org.commcare.util.screen.EntityListSubscreen;
import org.commcare.util.screen.EntityScreen;
import org.commcare.util.screen.MultiSelectEntityScreen;
import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.core.model.instance.TreeReference;
import org.javarosa.core.util.NoLocalizedTextException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.function.Predicate;

import datadog.trace.api.Trace;

/**
 * Created by willpride on 4/13/16.
 */
public class EntityListResponse extends MenuBean {
    private EntityBean[] entities;
    private DisplayElement[] actions;
    private String redoLast;
    private Style[] styles;
    private String[] headers;
    private Tile[] tiles;
    private int[] widthHints;
    private int numEntitiesPerRow;
    private boolean useUniformUnits;
    private int[] sortIndices;
    private String noItemsText;

    private int pageCount;
    private int currentPage;
    private final String type = "entities";

    private static int DEFAULT_CASES_PER_PAGE = 10;
    private static int MAX_CASES_PER_PAGE = 100;

    private boolean usesCaseTiles;
    private int maxWidth;
    private int maxHeight;

    private boolean isMultiSelect = false;
    private int maxSelectValue = -1;

    private boolean hasDetails = true;
    private QueryResponseBean queryResponseBean;

    public EntityListResponse() {
    }

    public EntityListResponse(EntityScreen nextScreen,
            String detailSelection,
            int offset,
            String searchText,
            int sortIndex,
            boolean isFuzzySearchEnabled,
            int casesPerPage) {
        SessionWrapper session = nextScreen.getSession();
        Detail detail = nextScreen.getShortDetail();
        EntityDatum neededDatum = (EntityDatum)session.getNeededDatum();
        EvaluationContext ec = nextScreen.getEvalContext();

        this.actions = processActions(nextScreen.getSession());
        this.redoLast = processRedoLast(nextScreen.getSession());

        // When detailSelection is not null it means we're processing a case detail, not a case
        // list.
        // We will shortcircuit the computation to just get the relevant detailSelection.
        if (detailSelection != null) {
            TreeReference reference = neededDatum.getEntityFromID(ec, detailSelection);
            if (reference == null) {
                throw new ApplicationConfigException(
                        String.format("Could not create detail %s for case with ID %s " +
                                        " either because the case filter matched zero or multiple"
                                        + " cases.",
                                detailSelection, detail));
            }
            Detail[] longDetails = nextScreen.getLongDetailList(reference);
            if (longDetails != null) {
                detail = longDetails[0];
            }
            entities = processEntitiesForCaseDetail(detail, reference, ec, neededDatum);
        } else {
            Vector<TreeReference> references = nextScreen.getReferences();
            List<Entity<TreeReference>> entityList = buildEntityList(detail, ec, references, searchText,
                    sortIndex, isFuzzySearchEnabled);

            if (casesPerPage == 0) {
                casesPerPage = DEFAULT_CASES_PER_PAGE;
            }
            casesPerPage = Math.min(casesPerPage, MAX_CASES_PER_PAGE);

            List<Entity<TreeReference>> entitesForPage = paginateEntities(entityList, detail, casesPerPage,
                    offset);
            List<EntityBean> entityBeans = processEntitiesForCaseList(entitesForPage, ec, neededDatum);
            entities = new EntityBean[entityBeans.size()];
            entityBeans.toArray(entities);
            setNoItemsText(getNoItemsTextLocaleString(detail));
            hasDetails = nextScreen.getLongDetail() != null;
        }

        processTitle(session);
        processCaseTiles(detail);
        this.styles = processStyles(detail);
        Pair<String[], int[]> pair = processHeader(detail, ec, sortIndex);
        this.headers = pair.first;
        this.widthHints = pair.second;
        this.sortIndices = detail.getOrderedFieldIndicesForSorting();
        isMultiSelect = nextScreen instanceof MultiSelectEntityScreen;
        if (isMultiSelect) {
            maxSelectValue = ((MultiSelectEntityScreen)nextScreen).getMaxSelectValue();
        }
        setQueryKey(session.getCommand());
        queryResponseBean = new QueryResponseBean(
                nextScreen.getQueryScreen(),
                nextScreen.getSession()
        );
    }


    private void processCaseTiles(Detail shortDetail) {
        DetailField[] fields = shortDetail.getFields();
        if (!shortDetail.usesEntityTileView()) {
            return;
        }
        tiles = new Tile[fields.length];
        setUsesCaseTiles(true);
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].isCaseTileField()) {
                tiles[i] = new Tile(fields[i]);
            } else {
                tiles[i] = null;
            }
        }
        numEntitiesPerRow = shortDetail.getNumEntitiesToDisplayPerRow();
        useUniformUnits = shortDetail.useUniformUnitsInCaseTile();
        Pair<Integer, Integer> maxWidthHeight = shortDetail.getMaxWidthHeight();
        maxWidth = maxWidthHeight.first;
        maxHeight = maxWidthHeight.second;
    }

    public static Pair<String[], int[]> processHeader(Detail shortDetail, EvaluationContext ec,
            int sortIndex) {
        return EntityListSubscreen.getHeaders(shortDetail, ec, sortIndex);
    }

    private static EntityBean[] processEntitiesForCaseDetail(Detail detail, TreeReference reference,
            EvaluationContext ec, EntityDatum neededDatum) {
        return new EntityBean[]{evalEntity(detail, reference, ec, neededDatum)};
    }

    @Trace
    public static List<EntityBean> processEntitiesForCaseList(List<Entity<TreeReference>> entityList,
            EvaluationContext ec,
            EntityDatum neededDatum) {
        List<EntityBean> entities = new ArrayList<>();
        for (Entity<TreeReference> entity : entityList) {
            entities.add(toEntityBean(entity, ec, neededDatum));
        }
        return entities;
    }

    @Trace
    private static List<Entity<TreeReference>> filterEntities(String searchText,
            NodeEntityFactory nodeEntityFactory,
            List<Entity<TreeReference>> full, boolean isFuzzySearchEnabled) {
        if (searchText != null && !"".equals(searchText)) {
            EntityStringFilterer filterer = new EntityStringFilterer(searchText.split(" "),
                    nodeEntityFactory, full, isFuzzySearchEnabled);
            full = filterer.buildMatchList();
        }
        return full;
    }

    private List<Entity<TreeReference>> paginateEntities(
            List<Entity<TreeReference>> entityList, Detail detail, int casesPerPage, int offset) {
        if (entityList.size() > casesPerPage && !(detail.getNumEntitiesToDisplayPerRow() > 1)) {
            // we're doing pagination
            return getEntitiesForCurrentPage(entityList, casesPerPage, offset);
        }
        return entityList;
    }

    @Trace
    private List<Entity<TreeReference>> getEntitiesForCurrentPage(List<Entity<TreeReference>> matched,
            int casesPerPage,
            int offset) {
        setPageCount((int)Math.ceil((double)matched.size() / casesPerPage));
        if (offset > matched.size()) {
            // Set the offset to last page
            offset = casesPerPage * (getPageCount() - 1);
        }
        setCurrentPage(offset / casesPerPage);

        int end = offset + casesPerPage;
        int length = casesPerPage;
        if (end > matched.size()) {
            end = matched.size();
            length = end - offset;
        }
        matched = matched.subList(offset, offset + length);
        return matched;
    }

    @Trace
    public static List<Entity<TreeReference>> buildEntityList(Detail shortDetail,
            EvaluationContext context,
            Vector<TreeReference> references,
            String searchText,
            int sortIndex,
            boolean isFuzzySearchEnabled) {
        NodeEntityFactory nodeEntityFactory = new NodeEntityFactory(shortDetail, context);
        List<Entity<TreeReference>> full = new ArrayList<>();
        for (TreeReference reference : references) {
            full.add(nodeEntityFactory.getEntity(reference));
        }
        nodeEntityFactory.prepareEntities(full);
        List<Entity<TreeReference>> matched = filterEntities(searchText, nodeEntityFactory, full,
                isFuzzySearchEnabled);
        sort(matched, shortDetail, sortIndex);
        return matched;
    }

    @Trace
    private static void sort(List<Entity<TreeReference>> entityList,
            Detail shortDetail,
            int sortIndex) {
        int[] order;
        boolean reverse = false;
        if (sortIndex != 0) {
            if (sortIndex < 0) {
                reverse = true;
                sortIndex = Math.abs(sortIndex);
            }
            // sort index is one indexed so adjust for that
            int sortFieldIndex = sortIndex - 1;
            order = new int[]{sortFieldIndex};
        } else {
            order = shortDetail.getOrderedFieldIndicesForSorting();
            for (int i = 0; i < shortDetail.getFields().length; ++i) {
                String header = shortDetail.getFields()[i].getHeader().evaluate();
                if (order.length == 0 && !"".equals(header)) {
                    order = new int[]{i};
                }
            }
        }
        java.util.Collections.sort(entityList,
                new EntitySorter(shortDetail.getFields(), reverse, order, new LogNotifier()));
    }

    public int[] getSortIndices() {
        return sortIndices;
    }

    public void setSortIndices(int[] sortIndices) {
        this.sortIndices = sortIndices;
    }

    public boolean isHasDetails() {
        return hasDetails;
    }

    static class LogNotifier implements EntitySortNotificationInterface {
        @Override
        public void notifyBadFilter(String[] args) {

        }
    }

    // Converts the Given Entity to EntityBean
    @Trace
    private static EntityBean toEntityBean(Entity<TreeReference> entity,
            EvaluationContext ec, EntityDatum neededDatum) {
        Object[] entityData = entity.getData();
        Object[] data = new Object[entityData.length];
        String id = getEntityId(entity.getElement(), neededDatum, ec);
        EntityBean ret = new EntityBean(id);
        for (int i = 0; i < entityData.length; i++) {
            data[i] = processData(entityData[i]);
        }
        ret.setData(data);
        return ret;
    }

    private static Object processData(Object data) {
        if (data instanceof GraphData) {
            try {
                return FormplayerGraphUtil.getHtml((GraphData)data, "").replace("\"", "'");
            } catch (GraphException e) {
                return "<html><body>Error loading graph " + e + "</body></html>";
            }
        } else {
            return data;
        }
    }

    // Evaluates detail fields for the given entity reference and returns it as EntityBean
    @Trace
    private static EntityBean evalEntity(Detail detail, TreeReference treeReference,
            EvaluationContext ec, EntityDatum neededDatum) {
        EvaluationContext context = new EvaluationContext(ec, treeReference);
        detail.populateEvaluationContextVariables(context);
        DetailField[] fields = detail.getFields();
        Object[] data = new Object[fields.length];
        String id = getEntityId(treeReference, neededDatum, ec);
        EntityBean ret = new EntityBean(id);
        int i = 0;
        for (DetailField field : fields) {
            Object o = field.getTemplate().evaluate(context);
            data[i] = processData(o);
            i++;
        }
        ret.setData(data);
        return ret;
    }

    private static String getEntityId(TreeReference treeReference, EntityDatum neededDatum, EvaluationContext ec) {
        return neededDatum == null ? "" : EntityScreen.getReturnValueFromSelection(
                treeReference, neededDatum, ec);
    }

    private static Style[] processStyles(Detail detail) {
        DetailField[] fields = detail.getFields();
        Style[] styles = new Style[fields.length];
        int i = 0;
        for (DetailField field : fields) {
            Style style = new Style(field);
            styles[i] = style;
            i++;
        }
        return styles;
    }

    private static DisplayElement[] processActions(SessionWrapper session) {
        Vector<Action> actions = getActionDefinitions(session);
        ArrayList<DisplayElement> displayActions = new ArrayList<>();
        for (Action action : actions) {
            displayActions.add(new DisplayElement(action, session.getEvaluationContext()));
        }
        DisplayElement[] ret = new DisplayElement[actions.size()];
        displayActions.toArray(ret);
        return ret;
    }

    private static String processRedoLast(SessionWrapper session) {
        return formatActionsWithFilter(session, Action::isRedoLast);
    }

    private static String formatActionsWithFilter(SessionWrapper session,
            Predicate<Action> actionFilter) {
        Vector<Action> actions = getActionDefinitions(session);
        String ret = null;
        int index = 0;
        for (Action action : actions) {
            if (actionFilter.test(action)) {
                ret = "action " + index;
            }
            index = index + 1;
        }
        return ret;
    }


    private static Vector<Action> getActionDefinitions(SessionWrapper session) {
        EntityDatum datum = (EntityDatum)session.getNeededDatum();
        return session.getDetail((datum).getShortDetail()).getCustomActions(
                session.getEvaluationContext());
    }

    public EntityBean[] getEntities() {
        return entities;
    }

    public void setEntities(EntityBean[] entities) {
        this.entities = entities;
    }

    public Style[] getStyles() {
        return styles;
    }

    public void setStyles(Style[] styles) {
        this.styles = styles;
    }

    public DisplayElement[] getActions() {
        return actions;
    }

    private void setActions(DisplayElement[] actions) {
        this.actions = actions;
    }

    public String getRedoLast() {
        return redoLast;
    }

    public void setRedoLast(String redoLast) {
        this.redoLast = redoLast;
    }

    @Override
    public String toString() {
        return "EntityListResponse [Title= " + getTitle() +
                ", noItemsText=" + getNoItemsText() +
                ", styles=" + Arrays.toString(styles) +
                ", action=" + Arrays.toString(actions) +
                ", parent=" + super.toString() +
                ", headers=" + Arrays.toString(headers) +
                ", locales=" + Arrays.toString(getLocales()) + "]";
    }

    public String[] getHeaders() {
        return headers;
    }

    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    public int[] getWidthHints() {
        return widthHints;
    }

    public void setWidthHints(int[] widthHints) {
        this.widthHints = widthHints;
    }

    public int getPageCount() {
        return pageCount;
    }

    private void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    private void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public String getType() {
        return type;
    }

    public boolean getUsesCaseTiles() {
        return usesCaseTiles;
    }

    public void setUsesCaseTiles(boolean usesCaseTiles) {
        this.usesCaseTiles = usesCaseTiles;
    }

    public Tile[] getTiles() {
        return tiles;
    }

    public int getNumEntitiesPerRow() {
        return numEntitiesPerRow;
    }

    public void setNumEntitiesPerRow(int numEntitiesPerRow) {
        this.numEntitiesPerRow = numEntitiesPerRow;
    }

    public int getMaxHeight() {
        return maxHeight;
    }

    public void setMaxHeight(int maxHeight) {
        this.maxHeight = maxHeight;
    }

    public int getMaxWidth() {
        return maxWidth;
    }

    public void setMaxWidth(int maxWidth) {
        this.maxWidth = maxWidth;
    }

    public boolean isUseUniformUnits() {
        return useUniformUnits;
    }

    public void setUseUniformUnits(boolean useUniformUnits) {
        this.useUniformUnits = useUniformUnits;
    }

    public boolean isMultiSelect() {
        return isMultiSelect;
    }

    public void setMultiSelect(boolean multiSelect) {
        isMultiSelect = multiSelect;
    }

    public int getMaxSelectValue() {
        return maxSelectValue;
    }

    public void setMaxSelectValue(int maxSelectValue) {
        this.maxSelectValue = maxSelectValue;
    }

    private void setNoItemsText(String noItemsText) {
        this.noItemsText = noItemsText;
    }

    public String getNoItemsText() {
        return noItemsText;
    }

    private String getNoItemsTextLocaleString(Detail detail) {
        String noItemsTextString;
        try {
            noItemsTextString = detail.getNoItemsText().evaluate();
        } catch (NoLocalizedTextException | NullPointerException e) {
            return null;
        }
        return noItemsTextString;
    }

    public QueryResponseBean getQueryResponseBean() {
        return queryResponseBean;
    }
}
