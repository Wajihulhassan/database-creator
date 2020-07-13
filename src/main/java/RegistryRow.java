import java.sql.Timestamp;

public class RegistryRow {

        String ID;
        String hostname;
        String action;
        String actorID;
        String objectID;
        Timestamp ts;
        String key_path;
        String key_type;
        String key_value;

        public RegistryRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp timestamp, String key_path, String key_type, String key_value) {
            this.ID = ID;
            this.hostname = hostname;
            this.action = action;
            this.actorID = actorID;
            this.objectID = objectID;
            this.ts = timestamp;
            this.key_path = key_path;
            this.key_type = key_type;
            this.key_value = key_value;
        }
}
