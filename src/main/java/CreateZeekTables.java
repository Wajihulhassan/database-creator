import com.github.wnameless.json.flattener.JsonFlattener;
import org.joda.time.Instant;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Map;

public class CreateZeekTables {

    private Connection connection;

    public CreateZeekTables(Connection connection) {
        this.connection = connection;

    }

    public void parseJsonFileWithoutOrder(String path)throws IOException, SQLException {
        System.out.println("-======= Parsing "+ path);
        File file = new File(path);
        FileReader fileReader;
        fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        while ((line = bufferedReader.readLine()) != null)
        {
            try {
                JSONObject JsonObject = new JSONObject(line);
                Map<String, Object> tmp = JsonFlattener.flattenAsMap(JsonObject.toString());
                insertJson(tmp);
            } catch (Exception e){
                System.out.println("Caught exception in json:  " + line);
                e.printStackTrace();
                System.exit(0);
            }
        }
        fileReader.close();
        System.out.println("Done inserting.....");
    }

    void addBroProcessTable() throws SQLException {
        Statement stmt = this.connection.createStatement();

        String drop_sql = "DROP TABLE IF EXISTS bro_process_events";
        stmt.executeUpdate(drop_sql);

        String sql = "CREATE TABLE bro_process_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
                "action varchar(10), actorID varchar(40), objectID varchar(40), bro_uid varchar(30), pid int)";
        stmt.executeUpdate(sql);

        String index_sql = "CREATE INDEX index_objectID on bro_process_events(objectID)";
        stmt.executeUpdate(index_sql);

        stmt.close();

    }

    void insertJson( Map<String, Object> jsonMap) throws SQLException {
        String ID = jsonMap.get("id").toString();
        String hostname = jsonMap.get("hostname").toString();
        String action = jsonMap.get("action").toString();
        String actorID = jsonMap.get("actorID").toString();
        String objectID = jsonMap.get("objectID").toString();
        String object = jsonMap.get("object").toString();
        String pid = jsonMap.get("pid").toString();
        String timestamp = jsonMap.get("timestamp").toString();
        Timestamp ts = new Timestamp(Instant.parse(timestamp).getMillis());
        String bro_uid = jsonMap.get("properties.bro_uid").toString();
        String sql = "insert into bro_process_events(id, timestamp, hostname, action, actorID, objectID, bro_uid)"
                    + "values (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, ID);
        ps.setTimestamp(2, ts);
        ps.setString(3, hostname);
        ps.setString(4,action);
        ps.setString(5,actorID);
        ps.setString(6,objectID);
        ps.setString(7,bro_uid);
        ps.setInt(7,Integer.valueOf(pid));

        ps.executeUpdate();
        ps.close();


    }
    }
