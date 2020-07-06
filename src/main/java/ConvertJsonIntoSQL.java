import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import org.joda.time.Instant;

import java.util.*;

import com.github.wnameless.json.flattener.JsonFlattener;
import org.json.JSONObject;

public class ConvertJsonIntoSQL {

    private Connection connection;
    private Map<String, ProcessEntities> all_actors_map;
    public ConvertJsonIntoSQL(Connection input_conn) {
        this.connection = input_conn;
        all_actors_map = new HashMap<>();
    }

    public void parseJsonFileWithoutOrder(String path)throws IOException, SQLException{
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
        addProcessEntities();
    }

    public void parseJsonFile(String path) throws IOException, SQLException {
        System.out.println("-======= Parsing "+ path);
        File file = new File(path);
        FileReader fileReader;
        fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;

        //ArrayList<Map<String, Object>> buffer = new ArrayList<>();
        Queue<Map<String, Object>> buffer = new PriorityQueue<>(new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> a1, Map<String, Object> a2) {
                try {
                    Instant t1 = Instant.parse(a1.get("timestamp").toString());
                    Instant t2 = Instant.parse(a2.get("timestamp").toString());
                    if(t1.compareTo(t2) > 0) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                } catch (Exception e) {
                    System.out.println("Caught Nullpointer in " + a1.toString());
                    System.out.println("Caught Nullpointer in " + a2.toString());
                    e.printStackTrace();
                    //System.exit(0);
                    return -1;
                }
            }
        });
        int  intial_stop = 0;
        while (true) {
            intial_stop = intial_stop +1;
            int counter = 0;
            // First I need to sort the events
            //buffer.clear();
            while ((line = bufferedReader.readLine()) != null && counter <100000) {
                //line = "{" + line.substring(line.indexOf("\"artifacts\""));
                JSONObject JsonObject = new JSONObject(line);
                Map<String,Object> tmp = JsonFlattener.flattenAsMap(JsonObject.toString());
                counter += 1;
                buffer.add(tmp);
                //System.out.println(tmp);
            }

            //Collections.sort(buffer, cmp);
            System.out.println("Buffer size " + buffer.size());
            //for (Map<String, Object> current: buffer){
            while (!buffer.isEmpty()){
                Map<String, Object> current = null;
                try {
                    current = buffer.poll();
                    insertJson(current);
                } catch (Exception e) {
                    System.out.println("Caught Nullpointer in " + current.toString());
                    e.printStackTrace();
                    System.exit(0);
                }
            }
            //Break the outer loop
            if (line ==  null){
                break;
            }
//            if (intial_stop > 1){
//                break;
//            }
        }
        fileReader.close();
        addProcessEntities();
    }

    public String getValue(Map<String, Object> jsonMap, String key){
        if (jsonMap.containsKey(key)){
            return jsonMap.get(key).toString();
        }
        return "Nan";
    }
    void addToProcess(String id, String pid, String image_path){
        if (all_actors_map.containsKey(id)){
            if (all_actors_map.get(id).image_path.equals("Nan") && !image_path.equals("Nan")){
                all_actors_map.get(id).image_path = image_path;
            }
        }else{
            all_actors_map.put(id,new ProcessEntities(pid,image_path));
        }

    }
    void addProcessEntities() throws SQLException {
        for (Map.Entry<String, ProcessEntities> entry: all_actors_map.entrySet()) {
            String actorID = entry.getKey();
            Integer pid = Integer.valueOf(entry.getValue().pid);
            String path = entry.getValue().image_path;
            String sql = "insert into process_entities(id, pid, path)"
                    + "values (?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, actorID);
            ps.setInt(2, pid);
            ps.setString(3, path);
            ps.executeUpdate();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            ps.close();
        }

    }

    void insertJson( Map<String, Object> jsonMap) throws SQLException, UnknownHostException {
        String ID = jsonMap.get("id").toString();
        String hostname = jsonMap.get("hostname").toString();
        String action = jsonMap.get("action").toString();
        String actorID = jsonMap.get("actorID").toString();
        String objectID = jsonMap.get("objectID").toString();
        String object = jsonMap.get("object").toString();
        String timestamp = jsonMap.get("timestamp").toString();
        Timestamp ts = new Timestamp(Instant.parse(timestamp).getMillis());
        String pid =  jsonMap.get("pid").toString();
        String ppid = jsonMap.get("ppid").toString();
        String image_path = getValue(jsonMap, "properties.image_path");

        if (object.equalsIgnoreCase("process")){
            if (action.equalsIgnoreCase("terminate")) {
                return;
            }
            String cmdline = this.getValue(jsonMap, "properties.command_line");
            String parent_path = this.getValue(jsonMap, "properties.parent_image_path");
            if (action.equalsIgnoreCase("create")) {
                addToProcess(actorID, ppid, parent_path);
                addToProcess(objectID, pid, image_path);
            }
            String sql = "insert into process_events(id, timestamp, hostname, action, actorID, objectID, command_line)"
                    + "values (?,?,?,?,?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, ID);
            ps.setTimestamp(2, ts);
            ps.setString(3, hostname);
            ps.setString(4,action);
            ps.setString(5,actorID);
            ps.setString(6,objectID);
            ps.setString(7,cmdline);
            ps.executeUpdate();
            ResultSet generatedKeys =  ps.getGeneratedKeys();
            ps.close();
            return;
        }
        if (object.equalsIgnoreCase("file")){
            if (action.equalsIgnoreCase("modify") || action.equalsIgnoreCase("rename") || action.equalsIgnoreCase("delete")) {
                return;
            }
            if (!jsonMap.containsKey("properties.file_path")){
                return;
            }
            addToProcess(actorID, pid, image_path);
            String file_path = jsonMap.get("properties.file_path").toString();
            String size = "";
            if (action.equalsIgnoreCase("write")) {
                size = jsonMap.get("properties.size").toString();
            }
            String sql = "insert into file_events(id, timestamp, hostname, action, actorID, objectID, file_path)"
                    + "values (?,?,?,?,?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, ID);
            ps.setTimestamp(2, ts);
            ps.setString(3, hostname);
            ps.setString(4,action);
            ps.setString(5,actorID);
            ps.setString(6,objectID);
            ps.setString(7,file_path);
            ps.executeUpdate();
            ResultSet generatedKeys =  ps.getGeneratedKeys();
            ps.close();
            return;
        }

        if (object.equalsIgnoreCase("module")){

            if (action.equalsIgnoreCase("unload")) {
                return;
            }
            String module_path = jsonMap.get("properties.module_path").toString();
            String sql = "insert into module_events(id, timestamp, hostname, action, actorID, objectID, module_path)"
                    + "values (?,?,?,?,?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, ID);
            ps.setTimestamp(2, ts);
            ps.setString(3, hostname);
            ps.setString(4,action);
            ps.setString(5,actorID);
            ps.setString(6,objectID);
            ps.setString(7,module_path);
            ps.executeUpdate();
            ResultSet generatedKeys =  ps.getGeneratedKeys();
            ps.close();
            return;
        }

        if (object.equalsIgnoreCase("registry")){
            String key_path = jsonMap.get("properties.key").toString();
            String key_type =  getValue(jsonMap,"properties.type");
            String key_value = getValue(jsonMap,"properties.value");
            String sql = "insert into registry_events(id, timestamp, hostname, action, actorID, objectID, key, type, value)"
                    + "values (?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, ID);
            ps.setTimestamp(2, ts);
            ps.setString(3, hostname);
            ps.setString(4,action);
            ps.setString(5,actorID);
            ps.setString(6,objectID);
            ps.setString(7,key_path);
            ps.setString(8,key_type);
            ps.setString(9,key_value);
            ps.executeUpdate();
            ResultSet generatedKeys =  ps.getGeneratedKeys();
            ps.close();
            return;
        }
        if (object.equalsIgnoreCase("flow")){
            if (!action.equalsIgnoreCase("start")) {
                return;
            }
            String srcip = jsonMap.get("properties.src_ip").toString();
            Integer srcport = Integer.valueOf(jsonMap.get("properties.src_port").toString());
            String dstip =  jsonMap.get("properties.dest_ip").toString();
            Integer dstport = Integer.valueOf(jsonMap.get("properties.dest_port").toString());
            String direction = jsonMap.get("properties.direction").toString();
            Integer protocol = Integer.valueOf(jsonMap.get("properties.l4protocol").toString());
            String sql = "insert into socket_events(id, timestamp, hostname, action, actorID, objectID, dest_ip,dest_port,src_ip,src_port,direction,l4protocol)"
                    + "values (?,?,?,?,?,?,?::inet,?,?::inet,?,?,?)";
            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, ID);
            ps.setTimestamp(2, ts);
            ps.setString(3, hostname);
            ps.setString(4,action);
            ps.setString(5,actorID);
            ps.setString(6,objectID);
            ps.setString(7,dstip);
            ps.setInt(8,dstport);
            ps.setString(9,srcip);
            ps.setInt(10,srcport);
            ps.setString(11,direction);
            ps.setInt(12,protocol);
            ps.executeUpdate();
            ResultSet generatedKeys =  ps.getGeneratedKeys();
            ps.close();
            return;
        }



    }
}
