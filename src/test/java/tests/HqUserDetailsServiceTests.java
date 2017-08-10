package tests;

import beans.auth.HqUserDetailsBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import services.HqUserDetailsService;
import services.impl.HqUserDetailsServiceImpl;
import util.Constants;
import utils.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RunWith(SpringRunner.class)
@RestClientTest(HqUserDetailsService.class)
@ContextConfiguration(classes = TestContext.class)
public class HqUserDetailsServiceTests {

    @Autowired
    private ObjectMapper objectMapper;

    private HqUserDetailsService service;

    @Autowired
    private MockRestServiceServer server;

    @Before
    public void setUp() throws Exception {
        String detailsString = "{" +
                "\"domains\":[\"domain\"]," +
                "\"djangoUserId\":1," +
                "\"username\":\"user@domain.commcarehq.org\"," +
                "\"authToken\":\"authToke\"," +
                "\"superUser\":false" +
                "}";

        // This is a sucky way of doing it but couldn't get the simple way to work. Suspect we're not following
        // Spring Boot conventions.
        // TODO: http://www.baeldung.com/restclienttest-in-spring-boot
        RestTemplate restTemplate = new RestTemplate();
        server = MockRestServiceServer.createServer(restTemplate);
        service = new HqUserDetailsServiceImpl(restTemplate);
        String host = "http://localhost";
        ReflectionTestUtils.setField(service, "objectMapper", new ObjectMapper());
        ReflectionTestUtils.setField(service, "host", host);
        ReflectionTestUtils.setField(service, "formplayerAuthKey", "secretkey");

        this.server.expect(requestTo(host + Constants.SESSION_DETAILS_VIEW))
                .andExpect(header("X-MAC-DIGEST", "v5/Bfr2kmxjbe5gFIexnY5Lo08cglL28nizvPBBpXCY="))
                .andRespond(withSuccess(detailsString, MediaType.APPLICATION_JSON));
    }

    @Test
    public void whenCallingGetUserDetails_thenClientMakesCorrectCall()
            throws Exception {
        HqUserDetailsBean details = this.service.getUserDetails("123abc");

        assertThat(details.getUsername()).isEqualTo("user@domain.commcarehq.org");
        assertThat(details.getDomains()).isEqualTo(new String[]{"domain"});
    }
}