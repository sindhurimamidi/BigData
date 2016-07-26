
import org.apache.pig.PigServer;

import java.io.IOException;

public class IPCount {

    public static void main(String[] args) {

        try {
            PigServer pigServer = new PigServer("local");
            getIPCount(pigServer);

        }
        catch(Exception e) {

        }
    }

    public static void getIPCount(PigServer pigServer) throws IOException {
           String inputFile = "access.log";
           pigServer.registerQuery("A = load 'access.log' using PigStorage('-') as (ipAddress, message);");
	   pigServer.registerQuery("grpd = group A by ipAddress;");
	   pigServer.registerQuery("ipCnt = foreach grpd { ip = A.ipAddress; generate group, COUNT(ip);};");
	   pigServer.store("ipCnt", "ipcount.out");
    }
}
