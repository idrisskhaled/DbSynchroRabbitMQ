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

public class HO {
    java.sql.Connection connection = null;

    public void retrieve(){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT * FROM `product sales`");
            while (results.next()) {
                String data = results.getString(1);
                System.out.println("Fetching data by column index for row " + results.getRow() + " : " + data);
                data = results.getString(2);
                System.out.println("Fetching data by column name for row " + results.getRow() + " : " + data);
            }
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void update(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("UPDATE `product sales` set ");
            //code here
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void insert(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("INSERT INTO `product sales`(`Date`, `Region`, `Product`, `deletedAt`, `Qty`, `Cost`, `Amt`) VALUES('') ");
            //code here
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void delete(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("DELETE from `product sales` WHERE id="+message.get("id"));
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
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
    }
}
