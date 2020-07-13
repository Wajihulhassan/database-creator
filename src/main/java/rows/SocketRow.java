package rows;

import java.sql.Timestamp;

public class SocketRow {
    public String ID;
    public String hostname;
    public String action;
    public String actorID;
    public String objectID;
    public Timestamp ts;
    public String dest_ip;
    public int dest_port;
    public String src_ip;
    public int src_port;
    public String direction;
    public int l4protocol;

    public SocketRow(String ID, String hostname, String action, String actorID, String objectID, Timestamp ts,
                     String dest_ip, int dest_port, String src_ip, int src_port, String direction, int l4protocol) {
        this.ID = ID;
        this.hostname = hostname;
        this.action = action;
        this.actorID = actorID;
        this.objectID = objectID;
        this.ts = ts;
        this.dest_ip = dest_ip;
        this.dest_port = dest_port;
        this.src_ip = src_ip;
        this.src_port = src_port;
        this.direction = direction;
        this.l4protocol = l4protocol;
    }
}
