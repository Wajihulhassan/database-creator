package utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class ESManager {



    public Client getClient(String host, int port) {
        try {
            Settings.Builder setting =Settings.builder().put("client.transport.sniff", false);
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
            return client;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}

