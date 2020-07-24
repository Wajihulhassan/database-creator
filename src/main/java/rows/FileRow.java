package rows;

import java.sql.Timestamp;

public class FileRow {
    public String ID;
    public String hostname;
    public String action;
    public String actorID;
    public String objectID;
    public Timestamp ts;
    public String file_path;
    public String ts_string;

    public FileRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp timestamp, String file_path, String ts_string) {
        this.ID = ID;
        this.hostname = hostname;
        this.action = action;
        this.actorID = actorID;
        this.objectID = objectID;
        this.ts = timestamp;
        this.file_path = file_path;
        this.ts_string = ts_string;
    }
}
