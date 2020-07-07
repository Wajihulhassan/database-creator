import java.sql.Timestamp;

public class SocketRow {
    String ID;
    String hostname;
    String action;
    String actorID;
    String objectID;
    Timestamp ts;
    String dest_ip;
    int dest_port;
    String src_ip;
    int src_port;
    String direction;
    int l4protocol;

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
