package cis5550.jobs;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class URLScore {
    private final String url;
    private double score;
    private final int depth;
    private String baseHost;
    private final List<String> breadcrumbs;


    public URLScore(String url, String baseUrl, List<String> parentBreadcrumbs, int depth) {
        this.url = url;
        this.depth = depth;
        this.breadcrumbs = new ArrayList<>(parentBreadcrumbs);
        this.breadcrumbs.add(url);

        try {
            URL urlObj = new URI(url).toURL();
            URL baseUrlObj = new URI(baseUrl).toURL();
            this.baseHost = baseUrlObj.getHost();
            calculateScore(urlObj);
        } catch (Exception e) {
            this.baseHost = "";
            this.score = 0.0;
        }
    }


    public double getScore() {
        return this.score;
    }

    public int getDepth() {

        return this.depth;
    }

    public void calculateScore(URL url) {



    }
}
