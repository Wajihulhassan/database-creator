package rows;

import java.sql.Timestamp;

public class ModuleRow {

    public String ID;
    public String hostname;
    public String action;
    public String actorID;
    public String objectID;
    public Timestamp ts;
    public String module_path;

    public ModuleRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp timestamp, String module_path) {
        this.ID = ID;
        this.hostname = hostname;
        this.action = action;
        this.actorID = actorID;
        this.objectID = objectID;
        this.ts = timestamp;
        this.module_path = module_path;
    }
}
