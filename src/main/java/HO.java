import java.awt.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection ;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;

public class HO {
    java.sql.Connection connection = null;
    JFrame frame;
    JPanel panel;
    String data[][] = new String[10][7];
    public void retrieve(){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT * FROM `product sales`");
            int i=0;
            while (results.next()) {
                data[i]= new String[]{results.getString(1),results.getString(2),results.getString(3),results.getString(4),results.getString(6),results.getString(7),results.getString(8)};
                i++;
            }
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void update(JSONObject message){
        try {
            PreparedStatement stmt = connection.prepareStatement("UPDATE `product sales` SET `Date`=?, `Region`=?, `Product`=?, `deletedAt`=?, `Qty`=?, `Cost`=?, `Amt`=? ");
            stmt.setString(2, (String) message.get("region"));
            stmt.setString(3, (String) message.get("product"));
            stmt.setDate(1, (Date) message.get("date"));
            stmt.setDate(4, (Date) message.get("deletedAt"));
            stmt.setInt(5, (Integer) message.get("qty"));
            stmt.setFloat(6,(Float)message.get("cost"));
            stmt.setFloat(7,(Float)message.get("amt"));
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Could not update data" + e.getMessage());
        }
    }
    public void insert(JSONObject message){
        try {
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO `product sales`(`Date`, `Region`, `Product`, `deletedAt`, `Qty`, `Cost`, `Amt`) VALUES (?,?,?,?,?,?,?)");
            stmt.setString(2, (String) message.get("region"));
            stmt.setString(3, (String) message.get("product"));
            stmt.setDate(1, (Date) message.get("date"));
            stmt.setDate(4, (Date) message.get("deletedAt"));
            stmt.setInt(5, (Integer) message.get("qty"));
            stmt.setFloat(6,(Float)message.get("cost"));
            stmt.setFloat(7,(Float)message.get("amt"));
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Could not insert data" + e.getMessage());
        }
    }
    public void delete(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("DELETE from `product sales` WHERE id="+message.get("id"));
        } catch (SQLException e) {
            System.out.println("Could not delete data" + e.getMessage());
        }
    }
    public void connection(){
        try {
            String driverName = "com.mysql.jdbc.Driver";
            Class.forName(driverName);
            String serverName = "localhost";
            String schema = "ho";
            String url = "jdbc:mysql://" + serverName + "/" + schema;
            String username = "root";
            String password = "";
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Successfully Connected to the database!");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not find the database driver " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("Could not connect to the database " + e.getMessage());
        }

    }

    public void basicConsume() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("exchange", "direct", true);
        String queueName = channel.queueDeclare().getQueue();
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            JSONParser parser = new JSONParser();
            JSONObject json = null;
            try {
                json = (JSONObject) parser.parse(message);
                if(json.get("operation").equals("update")){update(json);}
                else if(json.get("operation").equals("insert")){insert(json);}
                else if(json.get("operation").equals("delete")){delete(json);}
            } catch (ParseException e) {
                e.printStackTrace();
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
    public static void main(String[] args) {
        new HO();

    }
    public HO(){
        connection();
        retrieve();
        frame = new JFrame("HO");
        panel = new JPanel();
        String col[] = {"Id","Date","Region","Product","Qty","Cost","Amt"};
        JTable table = new JTable(data,col);
        JScrollPane pane = new JScrollPane(table);
        pane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        panel.add(pane);
        frame.add(panel);
        frame.setSize(500,150);
        frame.setUndecorated(true);
        frame.getRootPane().setWindowDecorationStyle(JRootPane.PLAIN_DIALOG);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);

    }
}
