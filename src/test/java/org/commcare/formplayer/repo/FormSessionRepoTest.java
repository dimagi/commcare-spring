package org.commcare.formplayer.repo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.commcare.formplayer.objects.FormSessionListView;
import org.commcare.formplayer.objects.FunctionHandler;
import org.commcare.formplayer.objects.FormDefinition;
import org.commcare.formplayer.objects.SerializableFormSession;
import org.commcare.formplayer.utils.JpaTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;

import javax.persistence.EntityManager;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@EnableJpaAuditing
public class FormSessionRepoTest {

    @Autowired
    FormSessionRepo formSessionRepo;

    @Autowired
    FormDefinitionRepo formDefinitionRepo;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    public void setUp() {
        jdbcTemplate.execute("DELETE from formplayer_sessions");
    }

    @Test
    public void testSaveAndLoad() {
        SerializableFormSession session = getSession();

        formSessionRepo.saveAndFlush(session);
        entityManager.clear(); // clear the EM cache to force a re-fetch from DB
        SerializableFormSession loaded = JpaTestUtils.unwrapProxy(
                formSessionRepo.getById(session.getId())
        );
        assertThat(loaded).usingRecursiveComparison().ignoringFields("dateCreated", "version").isEqualTo(session);
        Instant dateCreated = loaded.getDateCreated();
        assertThat(dateCreated).isNotNull();
        assertThat(loaded.getVersion()).isEqualTo(1);

        formSessionRepo.saveAndFlush(loaded);
        assertThat(loaded.getDateCreated()).isEqualTo(dateCreated);
        assertThat(loaded.getVersion()).isEqualTo(2);
    }

    @Test
    public void testGetListView() {
        SerializableFormSession session = getSession();
        Map<String, String> sessionData = session.getSessionData();
        formSessionRepo.save(session);
        List<FormSessionListView> userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserIsNullOrderByDateCreatedDesc(
                "momo", "domain", PageRequest.of(0, 10)
        );
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("More Momo");
        assertThat(userSessions.get(0).getDateCreated()).isEqualTo(session.getDateCreated());
        assertThat(userSessions.get(0).getSessionData()).isEqualTo(sessionData);
        assertThat(userSessions.get(0).getId()).isEqualTo(session.getId());
    }

    @Test
    public void testGetListView_Ordering() {
        // create and save 3 sessions, reverse order of creation, extract IDs
        Iterator<String> sessionIdIterator = Stream.of(getSession(), getSession(), getSession()).map((session) -> {
            formSessionRepo.save(session);
            return session;
        }).map(SerializableFormSession::getId).collect(Collectors.toCollection(LinkedList::new))
                .descendingIterator();
        ArrayList<String> sessionIds = Lists.newArrayList(sessionIdIterator);

        List<FormSessionListView> userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserIsNullOrderByDateCreatedDesc(
                "momo", "domain", PageRequest.of(0, 10)
        );
        assertThat(userSessions).extracting("id").containsExactlyElementsOf(
                sessionIds
        );
    }

    @Test
    public void testGetListView_filterByDomain() {
        formSessionRepo.save(getSession("domain1", "session1"));
        formSessionRepo.save(getSession("domain2", "session2"));
        List<FormSessionListView> userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserIsNullOrderByDateCreatedDesc(
                "momo", "domain1", PageRequest.of(0, 10)
        );
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("session1");

        userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserIsNullOrderByDateCreatedDesc(
                "momo", "domain2", PageRequest.of(0, 10)
        );
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("session2");
    }

    @Test
    public void testGetListView_filterByAsUser() {
        formSessionRepo.save(getSession("domain1", "session_user1", "asUser1"));
        formSessionRepo.save(getSession("domain1", "session_user2", "asUser2"));
        formSessionRepo.save(getSession("domain1", "session_momo", null));
        List<FormSessionListView> userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserOrderByDateCreatedDesc(
                "momo", "domain1", "asUser1", PageRequest.of(0, 10));
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("session_user1");

        userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserOrderByDateCreatedDesc(
                "momo", "domain1", "asUser2", PageRequest.of(0, 10));
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("session_user2");

        userSessions = formSessionRepo.findByUsernameAndDomainAndAsUserIsNullOrderByDateCreatedDesc(
                "momo", "domain1", PageRequest.of(0, 10));
        assertThat(userSessions).hasSize(1);
        assertThat(userSessions.get(0).getTitle()).isEqualTo("session_momo");
    }

    @Test
    public void testUpdateableFields() {
        SerializableFormSession session = getSession();

        // save session
        formSessionRepo.saveAndFlush(session);
        int version = session.getVersion();

        // update field that should not get updated in the DB
        ReflectionTestUtils.setField(session, "domain", "newdomain");
        formSessionRepo.saveAndFlush(session);
        entityManager.refresh(session);

        // check that version is updated
        assertThat(session.getVersion()).isGreaterThan(version);
        assertThat(session.getDomain()).isEqualTo("domain");
    }

    @Test
    public void testDeleteByDateCreatedLessThan() {
        Instant now = Instant.now();

        List<SerializableFormSession> sessions = IntStream.range(0, 5)
                .mapToObj(i -> getSession())
                .collect(Collectors.toList());
        List<SerializableFormSession> savedSessions = formSessionRepo.saveAll(sessions);
        entityManager.flush();
        entityManager.clear();

        for (int i = 0; i < savedSessions.size(); i++) {
            SerializableFormSession session = savedSessions.get(i);
            jdbcTemplate.update(
                    "UPDATE formplayer_sessions SET datecreated = ? where id = ?",
                    Timestamp.from(now.minus(i, ChronoUnit.DAYS)), session.getId()
            );
        }
        List<String> allIds = jdbcTemplate.queryForList("SELECT id FROM formplayer_sessions", String.class);
        assertThat(allIds.size()).isEqualTo(5);

        int deleted = formSessionRepo.deleteSessionsOlderThan(now.minus(2, ChronoUnit.DAYS));
        assertThat(deleted).isEqualTo(2);
        entityManager.flush();

        List<String> remainingIds = jdbcTemplate.queryForList("SELECT id FROM formplayer_sessions", String.class);
        assertThat(remainingIds.size()).isEqualTo(3);
    }

    @Test
    public void testFormDefinitionRelationship() {
        FormDefinition formDef = new FormDefinition(
                "appId",
                "appVersion",
                "formXmlns",
                "formXml"
        );
        formDefinitionRepo.save(formDef);
        SerializableFormSession session = getSession();
        session.setFormDefinition(formDef);
        formSessionRepo.save(session);
        FormDefinition fetchedFormDef = session.getFormDefinition();
        assertThat(fetchedFormDef.getAppId()).isEqualTo("appId");
        assertThat(fetchedFormDef.getDateCreated()).isEqualTo(formDef.getDateCreated());
        assertThat(fetchedFormDef.getAppVersion()).isEqualTo("appVersion");
        assertThat(fetchedFormDef.getXmlns()).isEqualTo("formXmlns");
    }

    private SerializableFormSession getSession() {
        return getSession("domain", "More Momo", null);
    }

    private SerializableFormSession getSession(String domain, String title) {
        return getSession(domain, title, null);
    }

    private SerializableFormSession getSession(String domain, String title, @Nullable String asUser) {
        FunctionHandler[] functionHandlers = {new FunctionHandler("count()", "123")};
        SerializableFormSession session = new SerializableFormSession(
                domain, "appId", "momo", asUser, "restoreAsCaseId",
                "/a/domain/receiver", null, title, true, "en", false,
                ImmutableMap.of("a", "1", "b", "2"),
                ImmutableMap.of("count", functionHandlers)
        );
        session.setInstanceXml("xml");
        session.setFormXml("form xml");
        return session;
    }
}
