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

public class HO {
    java.sql.Connection connection = null;
    JFrame frame;
    String column[] = {"Id","Date","Region","Product","Qty","Cost","Amt"};
    DefaultTableModel model;
    JPanel panel;
    JTable table;
    public ResultSet retrieve(){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT id ,date,Region,Product,Qty,Cost,Amt FROM `product sales` where deletedAt is null");
            return results;
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
        return null;
    }
    public void update(JSONObject message){
        try {
            PreparedStatement stmt = connection.prepareStatement("UPDATE `product sales` SET `Date`=?, `Region`=?, `Product`=?, `Qty`=?, `Cost`=?, `Amt`=? WHERE  `id`=?");
            stmt.setString(2, (String) message.get("region"));
            stmt.setString(3, (String) message.get("product"));
            stmt.setString(1, (String) message.get("date"));
            stmt.setString(4, (String) message.get("qty"));
            stmt.setString(5,(String)message.get("cost"));
            stmt.setString(6,(String)message.get("amt"));
            stmt.setString(7,(String)message.get("id") );
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Could not update data" + e.getMessage());
        }
    }
    public void insert(JSONObject message){
        try {
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO `product sales`(`Date`, `Region`, `Product`, `Qty`, `Cost`, `Amt`) VALUES (?,?,?,?,?,?)");
            stmt.setString(2, (String) message.get("region"));
            stmt.setString(3, (String) message.get("product"));
            stmt.setString(1, (String) message.get("date"));
            stmt.setString(4, (String) message.get("qty"));
            stmt.setString(5,(String)message.get("cost"));
            stmt.setString(6,(String)message.get("amt"));
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Could not insert data" + e.getMessage());
        }
    }
    public void delete(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("DELETE from `product sales` WHERE id="+message.get("id"));
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
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, "exchange", "bo1-ho");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            JSONParser parser = new JSONParser();
            JSONObject json;
            try {
                json = (JSONObject) parser.parse(message);
                if(json.get("operation").equals("update")){update(json);}
                else if(json.get("operation").equals("insert")){insert(json);}
                else if(json.get("operation").equals("delete")){delete(json);}
                afficher();
                if(json.get("sender").equals("bo2"))EmitLog(json,"ho-bo1");
                else if(json.get("sender").equals("bo1"))EmitLog(json,"ho-bo2");
            } catch (ParseException e) {
                e.printStackTrace();
            }
        };
        channel.basicConsume(queueName1, true, deliverCallback, consumerTag -> {
        });
        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, "exchange", "bo2-ho");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            JSONParser parser = new JSONParser();
            JSONObject json;
            try {
                json = (JSONObject) parser.parse(message);
                if(json.get("operation").equals("update")){update(json);}
                else if(json.get("operation").equals("insert")){insert(json);}
                else if(json.get("operation").equals("delete")){delete(json);}
                afficher();
                if(json.get("sender").equals("bo2"))EmitLog(json,"ho-bo1");
                else if(json.get("sender").equals("bo1"))EmitLog(json,"ho-bo2");
            } catch (ParseException e) {
                e.printStackTrace();
            }
        };
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> {
        });
    }
    public static void main(String[] args) throws SQLException, IOException, TimeoutException {
        new HO();
    }
    public void EmitLog(JSONObject obj, String Queue_name) throws IOException {
        String message = obj.toString();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare("exchange", "direct", true);
            channel.basicPublish("exchange", Queue_name, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

    }
    public void afficher() {
        ResultSet results=this.retrieve();
        model=new DefaultTableModel(column,0);
        String data[]=new String[7];
        try {
            while (results.next()) {
                for(int j=1;j<8;j++) {
                    String row = results.getString(j);
                    data[j-1]=row;
                }
                model.addRow(data);
            }
            table.setModel(model);

        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
    public HO() throws SQLException, IOException, TimeoutException {
        connection();
        model=new DefaultTableModel(column,0);
        frame = new JFrame("HO");
        panel = new JPanel();
        table = new JTable(model);
        JScrollPane pane = new JScrollPane(table);
        panel.add(pane);
        frame.add(panel);
        frame.setSize(500,150);
        frame.setUndecorated(true);
        frame.getRootPane().setWindowDecorationStyle(JRootPane.PLAIN_DIALOG);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
        afficher();
        basicConsume();

    }
}
