import java.sql.Timestamp;

public class ProcessRow {
    String ID;
    String hostname;
    String action;
    String actorID;
    String objectID;
    Timestamp ts;
    String command_line;

    public ProcessRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp timestamp, String command_line) {
        this.ID = ID;
        this.hostname = hostname;
        this.action = action;
        this.actorID = actorID;
        this.objectID = objectID;
        this.ts = timestamp;
        this.command_line = command_line;
    }
}
