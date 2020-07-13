import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import utils.ESManager;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;

public class QueryES {
    ESManager es;
    Client client;

    public QueryES(String hostname, int port) {
        this.es = new ESManager();
        client = es.getClient(hostname,port);
    }


    public List<String> getPhraseQueryData() {
        QueryBuilder query = matchPhraseQuery("uid", "Cm4CXF105KrOvHDrh2");
        System.out.println("getPhraseQueryCount query =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("2019-09-25").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            System.out.println(hit.getSourceAsString());
            list.add(hit.getSourceAsString());
        }
        return list;
    }
    public List<String> getMatchAllQueryData() {
        QueryBuilder query = matchAllQuery();
        System.out.println("getMatchAllQueryCount query =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

}
