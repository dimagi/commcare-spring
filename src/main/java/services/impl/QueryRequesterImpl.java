package services.impl;

import annotations.MethodMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import services.QueryRequester;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by willpride on 4/26/17.
 */
public class QueryRequesterImpl implements QueryRequester {

    private final Log log = LogFactory.getLog(QueryRequesterImpl.class);

    @Override
    @MethodMetrics
    public String makeQueryRequest(String uri, HttpHeaders headers) {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response =
                null;
        try {
            response = restTemplate.exchange(
                    // Spring framework automatically encodes urls. This ensures we don't pass in an already
                    // encoded url.
                    URLDecoder.decode(uri, "UTF-8"),
                    HttpMethod.GET,
                    new HttpEntity<String>(headers),
                    String.class
            );
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String responseBody = response.getBody();
        log.info(String.format("Query request to URL %s returned result %s", uri, responseBody));
        return responseBody;
    }
}
