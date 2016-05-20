package beans.menus;

import io.swagger.annotations.ApiModel;
import org.commcare.modern.session.SessionWrapper;
import org.commcare.suite.model.Action;
import org.commcare.suite.model.Detail;
import org.commcare.suite.model.DetailField;
import org.commcare.suite.model.EntityDatum;
import org.commcare.util.cli.EntityDetailSubscreen;
import org.commcare.util.cli.EntityScreen;
import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.core.model.instance.TreeReference;
import org.javarosa.xpath.XPathException;
import util.SessionUtils;

import java.util.Arrays;
import java.util.Vector;

/**
 * Created by willpride on 4/13/16.
 */
@ApiModel("Entity List Response")
public class EntityListResponse extends MenuBean {
    private Entity[] entities;
    private DisplayElement action;
    private Style[] styles;

    public EntityListResponse(){}

    public EntityListResponse(EntityScreen nextScreen) {
        SessionWrapper session = nextScreen.getSession();
        Detail shortDetail = nextScreen.getShortDetail();
        EvaluationContext ec = session.getEvaluationContext();
        Vector<TreeReference> references = ec.expandReference(((EntityDatum)session.getNeededDatum()).getNodeset());
        processTitle(session);
        processEntities(nextScreen, references, ec);
        processStyles(shortDetail);
        processActions(shortDetail, nextScreen.getSession());
    }

    private void processTitle(SessionWrapper session) {
        setTitle(SessionUtils.getBestTitle(session));
    }

    private void processEntities(EntityScreen screen, Vector<TreeReference> references, EvaluationContext ec) {
        entities = new Entity[references.size()];
        int i = 0;
        for (TreeReference entity : references) {
            Entity newEntity = processEntity(entity, screen, ec);
            newEntity.setDetail(new EntityDetailResponse(processDetails(screen, ec, entity)));
            entities[i] = newEntity;
            i++;
        }
    }

    private Entity processEntity(TreeReference entity, EntityScreen screen, EvaluationContext ec) {
        Detail detail = screen.getShortDetail();
        Entity ret = new Entity();
        EvaluationContext context = new EvaluationContext(ec, entity);

        detail.populateEvaluationContextVariables(context);

        DetailField[] fields = detail.getFields();
        Object[] data = new Object[fields.length];

        int i = 0;
        for (DetailField field : fields) {
            Object o;
            try {
                o = field.getTemplate().evaluate(context);
            } catch(XPathException e) {
                throw new RuntimeException(e);
            }
            data[i] = o;
            i ++;
        }
        ret.setData(data);
        return ret;
    }

    private EntityDetailSubscreen processDetails(EntityScreen screen, EvaluationContext ec, TreeReference ref){
        EvaluationContext subContext = new EvaluationContext(ec, ref);
        //TODO WSP: don't hardcode first screen
        return new EntityDetailSubscreen(0, screen.getLongDetailList()[0],
                subContext, screen.getDetailListTitles(subContext));
    }

    private void processStyles(Detail detail) {
        DetailField[] fields = detail.getFields();
        styles = new Style[fields.length];
        int i = 0;
        for (DetailField field : fields) {
            Style style = new Style(field);
            styles[i] = style;
            i++;
        }
    }

    private void processActions(Detail detail, SessionWrapper session){
        Vector<Action> actions = session.getDetail(((EntityDatum)session.getNeededDatum()).getShortDetail()).getCustomActions();
        // Assume we only have one TODO WSP: is that correct?
        if(actions != null && !actions.isEmpty()) {
            Action action = actions.firstElement();
            setAction(new DisplayElement(action, session.getEvaluationContext()));
        }
    }

    public Entity[] getEntities() {
        return entities;
    }

    public void setEntities(Entity[] entities) {
        this.entities = entities;
    }

    public Style[] getStyles() {
        return styles;
    }

    public void setStyles(Style[] styles) {
        this.styles = styles;
    }

    public DisplayElement getAction() {
        return action;
    }

    public void setAction(DisplayElement action) {
        this.action = action;
    }

    @Override
    public String toString(){
        return "EntityListResponse [Entities=" + Arrays.toString(entities) + ", styles=" + Arrays.toString(styles) +
                ", action=" + action + " parent=" + super.toString() +"]";
    }
}
