import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MainPublic {
    static final String sshHost = "white.kent.ac.uk";
    static final int sshPort = 22;
    static final String sshUsername = "*****";
    static final String sshPassword = "*****";

    static final int dbPort = 3306;
    static final String dbUsername = "*****";
    static final String dbPassword = "*****";

    static final int localPort = 2358;

    public static MessageType[] initializeMessageLib() throws IOException, JSONException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("./InputFiles/BenAriErrorTypes.json")));
        StringBuilder stringBuilder = new StringBuilder("");
        String thisLine = bufferedReader.readLine();
        stringBuilder.append(thisLine);
        while (thisLine != null) {
            thisLine = bufferedReader.readLine();
            stringBuilder.append(thisLine);
        }

        List<MessageType> messageTypes = new ArrayList<>();
        JSONArray jsonArray = new JSONArray(stringBuilder.toString());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            int subtype = 0;
            if (jsonObject.has("Subtype")) {
                subtype = jsonObject.getInt("Subtype");
            }
            messageTypes.add(new MessageType(jsonObject.getInt("Category"), jsonObject.getInt("Type"), subtype, jsonObject.getString("Description"), jsonObject.getString("Regex")));
        }


        bufferedReader = new BufferedReader(new FileReader(new File("./InputFiles/MyErrorTypes.json")));
        stringBuilder = new StringBuilder("");
        thisLine = bufferedReader.readLine();
        stringBuilder.append(thisLine);
        while (thisLine != null) {
            thisLine = bufferedReader.readLine();
            stringBuilder.append(thisLine);
        }

        jsonArray = new JSONArray(stringBuilder.toString());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            int subtype = 0;
            if (jsonObject.has("Subtype")) {
                subtype = jsonObject.getInt("Subtype");
            }
            messageTypes.add(new MessageType(jsonObject.getInt("Category"), jsonObject.getInt("Type"), subtype, jsonObject.getString("Description"), jsonObject.getString("Regex")));
        }

        MessageType[] toReturn = new MessageType[messageTypes.size()];
        for (int i = 0; i < toReturn.length; i++) {
            toReturn[i] = messageTypes.get(i);
        }
        return toReturn;

    }

    public static void main(String[] args) {
        Session jschSession = null;
        MessageType[] messageTypeLib = null;

        try {
            messageTypeLib = initializeMessageLib();
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
        if (messageTypeLib == null) {
            System.out.println("MessageTypeLib not initialized.");
            return;
        } else {
            System.out.println("MessageLib initialized.");
        }

        try {
            JSch jsch = new JSch();
            jschSession = jsch.getSession(sshUsername, sshHost, sshPort);
            jschSession.setPassword(sshPassword);
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.connect();
            System.out.println("SSH connected. Printing SSH server version status:");
            System.out.println(jschSession.getServerVersion());
            int assinged_port = jschSession.setPortForwardingL(localPort, "127.0.0.1", dbPort);
            System.out.println("localhost:" + assinged_port + " -> " + "white.kent.ac.uk" + ":" + dbPort);
        } catch (Exception e) {
            e.printStackTrace();
            if (jschSession != null) {
                jschSession.disconnect();
            }
        }


        long time1, time2;
        try {
            Date startDate = AnalyzeMission.sdf.parse(args[0]);
            int duration = Integer.parseInt(args[1]);
            //AnalyzeMission analyzeMission = new AnalyzeMission(AnalyzeMission.sdf.parse("2016-02-01"), 29, "US", messageTypeLib);
            AnalyzeMission analyzeMission = new AnalyzeMission(startDate, duration, "US", messageTypeLib);
            time1 = System.currentTimeMillis();
            analyzeMission.doAnalysis();
            time2 = System.currentTimeMillis();
            System.out.println("Time used for analysis: " + (time2 - time1) + " ms");

            time1 = System.currentTimeMillis();
            analyzeMission.recordResult();
            time2 = System.currentTimeMillis();
            System.out.println("Time used for record: " + (time2 - time1) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (jschSession != null) {
            jschSession.disconnect();
        }

    }


}
