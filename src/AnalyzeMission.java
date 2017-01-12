import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by TTY on 2016/8/15.
 */
public class AnalyzeMission {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Date startTime;
    int dataSetSize;
    String countryCode;
    MessageType[] messageTypeLib;
    //Map<String, Integer> totalMap = new HashMap<>();
    //Map<String, Integer> shownMap = new HashMap<>();
    int[] bothTotal, shownTotal;
    int[][] relationShip;
    Map<String, Integer> uctgrzdSMap = new HashMap<>();
    Map<String, Integer> uctgrzdBMap = new HashMap<>();
    ConcurrentLinkedDeque<String> sessionIdList = new ConcurrentLinkedDeque<>();
    int totalSessionNum = 0;
    int totalCompileEventNum = 0;
    private int session_num = 100;
    private File currentRoot;
    private PrintStream p_ctg = null, p_unctg = null;
    private boolean isOver1 = false;
    private boolean isOver2 = false;

    AnalyzeMission(Date startTime, int dataSetSize, String countryCode, MessageType[] messageTypeLib) {
        this.startTime = startTime;
        this.dataSetSize = dataSetSize;
        this.countryCode = countryCode;

        String primitiveFolderName = countryCode + sdf.format(startTime) + "-" + sdf.format(new Date(startTime.getTime() + ((long) dataSetSize) * 24 * 60 * 60 * 1000));
        String folderName = primitiveFolderName;
        this.currentRoot = new File("./OutputFiles/" + folderName);
        int repeatNum = 0;
        while (currentRoot.exists()) {
            repeatNum++;
            folderName = primitiveFolderName + "(" + repeatNum + ")";
            this.currentRoot = new File("./OutputFiles/" + folderName);
        }
        this.messageTypeLib = messageTypeLib;
        try {
            currentRoot.mkdir();
            p_ctg = new PrintStream(new FileOutputStream(new File("./OutputFiles/" + folderName + "/CategorizedMessages.txt")));
            p_unctg = new PrintStream(new FileOutputStream(new File("./OutputFiles/" + folderName + "/UncategorizedMessages.txt")));
            for (MessageType messageType : messageTypeLib) {
                new File("./OutputFiles/" + folderName + "/DetailedMessages").mkdir();
                messageType.detailMessageWriter = new PrintStream(new FileOutputStream(new File("./OutputFiles/" + folderName + "/DetailedMessages/" + messageType.category + "_" + messageType.type + ((messageType.subtype != 0) ? ("_" + messageType.subtype) : ("")) + ".txt")));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (p_ctg == null || p_unctg == null) {
            System.out.println("Cannot initialize file output.");
        } else {
            System.out.println("File output initialized.");
        }
        shownTotal = new int[messageTypeLib.length];
        bothTotal = new int[messageTypeLib.length];
        relationShip = new int[messageTypeLib.length + 1][];
        for (int i = 0; i < relationShip.length; i++) {
            relationShip[i] = new int[messageTypeLib.length + 1];
        }

    }

    void doAnalysis() {
        new Thread(new fetchSessionIdTask()).start();
        new Thread(new fetchMessageTask()).start();
        while (!(isOver1 && isOver2)) {
            Thread.yield();
        }
    }

    void recordResult() throws Exception {
        XLSXWriter xlsxWriter = new XLSXWriter(this);
        xlsxWriter.writeToFile(currentRoot.getPath());

        for (int i = 0; i < messageTypeLib.length; i++) {
            MessageType messageType = messageTypeLib[i];
            p_ctg.println(messageType.category + "." + messageType.type + ((messageType.subtype != 0) ? ("." + messageType.subtype) : ("")) + "\t" + messageType.description + ": " + shownTotal[i] + "/" + bothTotal[i]);
        }
        p_ctg.close();

        System.out.println(uctgrzdBMap.size());
        List<Map.Entry<String, Integer>> listedUctgrzdBMap = new ArrayList<>(uctgrzdBMap.entrySet());
        for (Map.Entry<String, Integer> aListedMap : listedUctgrzdBMap) {
            p_unctg.println(aListedMap.getKey() + " : " + +(uctgrzdSMap.containsKey(aListedMap.getKey()) ? uctgrzdSMap.get(aListedMap.getKey()) : 0) + "/" + aListedMap.getValue());
        }
        p_unctg.close();

        for (MessageType messageType : messageTypeLib) {
            messageType.detailMessageWriter.close();
        }
    }

    private int addDataToMap(String message, int shown) {
        if (message.startsWith("Some messages have been simplified")) {
            return -1;
        }
        if (message.matches(".*[\u3040-\u309F]+[\\s\\S]*") || message.matches(".*[\u30A0-\u30FF]+[\\s\\S]*") || message.matches(".*[\u4E00-\u9FBF]+[\\s\\S]*")) {
            //Exclude all the error messages with Chinese or Japanese
            return -1;
        }
        for (int i = 0; i < messageTypeLib.length; i++) {
            MessageType messageType = messageTypeLib[i];
            if (message.matches(messageType.regex)) {
                messageType.detailMessageWriter.println(message);
                bothTotal[i]++;
                if (shown == 1) {
                    shownTotal[i]++;
                }
                return i;
            }
        }
        if (uctgrzdBMap.containsKey(message)) {
            uctgrzdBMap.put(message, uctgrzdBMap.get(message) + 1);
        } else {
            uctgrzdBMap.put(message, 1);
        }

        if (shown == 1) {
            if (uctgrzdSMap.containsKey(message)) {
                uctgrzdSMap.put(message, uctgrzdSMap.get(message) + 1);
            } else {
                uctgrzdSMap.put(message, 1);
            }
        }
        return -1;
    }

    private void addRelationshipToMap(int shownMessageID, List<Integer> messageIDList) {
        if (shownMessageID == -1) {
            System.out.println("No shown message with " + messageIDList.size() + " messages followed.");
            totalCompileEventNum--;
            return;
        }
        for (int messageID : messageIDList) {
            relationShip[shownMessageID][messageID]++;
        }
    }

    class fetchSessionIdTask implements Runnable {
        @Override
        public void run() {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            Connection conn;
            Statement st;
            ResultSet rs;
            try {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:" + MainSecret.localPort + "/blackbox_production", MainSecret.dbUsername, MainSecret.dbPassword);
                st = conn.createStatement();
                for (int i = 0; i < dataSetSize; i++) {
                    String sql = "select * from(select session_id, country_code FROM (blackbox_production.master_events left join geocoded.geocodes on blackbox_production.master_events.client_address_id = geocoded.geocodes.client_address_id) where master_events.name = 'bluej_start' and master_events.created_at between '" + sdf.format(new Date(startTime.getTime() + (long) i * 24 * 60 * 60 * 1000)) + "' and '" + sdf.format(new Date(startTime.getTime() + (long) (i + 1) * 24 * 60 * 60 * 1000)) + "') as M where M.country_code = '" + countryCode + "';";
                    rs = st.executeQuery(sql);
                    while (rs.next()) {
                        sessionIdList.add(rs.getString("session_id"));
                        totalSessionNum++;
                    }
                    System.out.println("[" + (i + 1) + "/" + dataSetSize + "]Current session ID num:" + sessionIdList.size());
                    if (sessionIdList.size() >= 5000) {
                        Thread.yield();
                    }
                }
                isOver1 = true;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    class fetchMessageTask implements Runnable {
        @Override
        public void run() {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            Connection conn;
            Statement st;
            ResultSet rs;
            try {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:" + MainSecret.localPort + "/blackbox_production", MainSecret.dbUsername, MainSecret.dbPassword);
                st = conn.createStatement();
                while (true) {
                    if (isOver1) {
                        while (!sessionIdList.isEmpty()) {
                            String query = "SELECT * FROM master_events left join compile_events on master_events.event_id = compile_events.id left join compile_outputs on compile_events.id = compile_outputs.compile_event_id where (";

                            int counter = 0;
                            while (counter < session_num && !sessionIdList.isEmpty()) {
                                if (counter != 0) {
                                    query += " or ";
                                }
                                query += "master_events.session_id = '" + sessionIdList.pop() + "'";
                                counter++;
                            }
                            query += ") and master_events.name = 'compile' and compile_events.success = 0 and compile_outputs.is_error = 1;";
                            rs = st.executeQuery(query);
                            long event_id = -1;
                            int shownMessageID = -1;
                            List<Integer> messageIDList = new ArrayList<>();
                            while (rs.next()) {
                                String message = rs.getString("message");
                                if (message != null) {
                                    int messageID = addDataToMap(message, rs.getInt("shown"));
                                    if (messageID == -1) {
                                        //System.out.println(rs.getString("message"));
                                    } else {
                                        long currentEvent_id = rs.getLong("event_id");
                                        if (event_id == -1) {
                                            event_id = currentEvent_id;
                                            totalCompileEventNum++;
                                        }
                                        if (event_id != currentEvent_id) {
                                            totalCompileEventNum++;
                                            addRelationshipToMap(shownMessageID, messageIDList);
                                            event_id = currentEvent_id;
                                            shownMessageID = -1;
                                            messageIDList = new ArrayList<>();
                                        }
                                        if (rs.getInt("shown") == 1) {
                                            if (shownMessageID != -1) {
                                                System.out.println("Multiple shown messages are followed.");
                                            }
                                            shownMessageID = messageID;
                                        } else {
                                            messageIDList.add(messageID);
                                        }
                                    }
                                }

                                //System.out.println(rs.getLong("event_id") + ">" + rs.getInt("sequence_num"));
                            }
                            if (shownMessageID != -1) {
                                addRelationshipToMap(shownMessageID, messageIDList);
                            }
                            System.out.println("Current session ID num:" + sessionIdList.size());
                        }
                        isOver2 = true;
                        break;
                    } else {
                        if (sessionIdList.isEmpty()) {
                            Thread.yield();
                        } else {
                            if (sessionIdList.size() <= session_num) {
                                Thread.yield();
                            } else {
                                String query = "SELECT * FROM master_events left join compile_events on master_events.event_id = compile_events.id left join compile_outputs on compile_events.id = compile_outputs.compile_event_id where (";

                                int counter = 0;
                                while (counter < session_num && !sessionIdList.isEmpty()) {
                                    if (counter != 0) {
                                        query += " or ";
                                    }
                                    query += "master_events.session_id = '" + sessionIdList.pop() + "'";
                                    counter++;
                                }
                                query += ") and master_events.name = 'compile' and compile_events.success = 0 and compile_outputs.is_error = 1;";
                                rs = st.executeQuery(query);

                                long event_id = -1;
                                int shownMessageID = -1;
                                List<Integer> messageIDList = new ArrayList<>();
                                while (rs.next()) {
                                    String message = rs.getString("message");
                                    if (message != null) {
                                        long currentEvent_id = rs.getLong("event_id");
                                        int messageID = addDataToMap(message, rs.getInt("shown"));
                                        if (messageID == -1) {
                                            //System.out.println(rs.getString("message"));
                                        } else {
                                            if (event_id == -1) {
                                                event_id = currentEvent_id;
                                                totalCompileEventNum++;
                                            }
                                            if (event_id != currentEvent_id) {
                                                totalCompileEventNum++;
//                                                System.out.println(shownMessageID);
//                                                for (int amessageID : messageIDList) {
//                                                    System.out.print(amessageID + "\t");
//                                                }
//                                                System.out.println("<");
                                                addRelationshipToMap(shownMessageID, messageIDList);

                                                event_id = currentEvent_id;
                                                shownMessageID = -1;
                                                messageIDList = new ArrayList<>();
                                            }
                                            if (rs.getInt("shown") == 1) {
                                                if (shownMessageID != -1) {
                                                    System.out.println("Multiple shown messages are followed.");
                                                }
                                                shownMessageID = messageID;
                                            } else {
                                                messageIDList.add(messageID);
                                            }
                                        }
                                    }

                                    //System.out.println(rs.getLong("event_id") + ">" + rs.getInt("sequence_num"));
                                }
                                addRelationshipToMap(shownMessageID, messageIDList);

                                System.out.println("Current session ID num:" + sessionIdList.size());
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
