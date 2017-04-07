import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.http.server.QueryMap;
import com.datastax.simulacron.common.cluster.QueryPrime.When;
import com.datastax.simulacron.common.cluster.QueryPrime.Then;
import com.datastax.simulacron.common.cluster.QueryPrime.Then.Row;
import com.datastax.simulacron.common.cluster.QueryPrime.Then.Column_types;
import org.junit.Test;

public class QueryMapTest {

  @Test
  public void testQueryMapInsert() throws Exception {
    QueryMap map = new QueryMap();
    String[] consistency = {"ONE"};
    QueryPrime query1 =
        new QueryPrime(
            new When("Real query", consistency),
            new Then(new Row[0], "target", new Column_types()));
    QueryPrime query2 =
        new QueryPrime(
            new When("Fake query", consistency),
            new Then(new Row[0], "target", new Column_types()));
    QueryPrime query3 =
        new QueryPrime(
            new When("Fake query", consistency),
            new Then(new Row[0], "target", new Column_types()));
    map.putQuery(1, 2, 3, query1);
    map.putQuery(1, 2, 3, query1);
    map.putQuery(1, 2, 3, query2);
  }
}
