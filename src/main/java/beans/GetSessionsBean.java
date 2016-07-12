package beans;

/**
 * POST request for getting a list of the user's incomplete form sessions
 */
public class GetSessionsBean {
    private String username;
    private String domain;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
