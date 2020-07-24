package rows;

import java.sql.Timestamp;

public class ProcessRow {
    public String ID;
    public String hostname;
    public String action;
    public String actorID;
    public String objectID;
    public Timestamp ts;
    public String command_line;
    public String ts_string;

    public ProcessRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp timestamp, String command_line, String ts_string) {
        this.ID = ID;
        this.hostname = hostname;
        this.action = action;
        this.actorID = actorID;
        this.objectID = objectID;
        this.ts = timestamp;
        this.command_line = command_line;
        this.ts_string = ts_string;
    }
}
