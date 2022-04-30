import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class BO2 implements ActionListener{
    java.sql.Connection connection = null;
    JFrame f;
    JButton rollback;
    DefaultTableModel model;
    DefaultTableModel model2;
    JButton submit1;
    JButton submit2;
    JButton submit3;
    JButton submit4;
    JPanel panel1;
    JPanel panel2;
    JPanel panel3;
    JPanel panel4;
    JPanel panel5;
    JLabel id;
    JLabel id1;
    JLabel id2;
    JLabel date;
    JLabel region;
    JLabel product;
    JLabel qty;
    JLabel cost;
    JLabel amt;
    JLabel date1;
    JLabel region1;
    JLabel product1;
    JLabel qty1;
    JLabel cost1;
    JLabel amt1;
    JTable jt;
    JTable jt2;
    JLabel deleted;
    JTextField idt;
    JTextField idt1;
    JTextField idt2;
    JTextField datet;
    JTextField regiont;
    JTextField productt;
    JTextField qtyt;
    JTextField costt;
    JTextField amtt;
    JTextField datet1;
    JTextField regiont1;
    JTextField productt1;
    JTextField qtyt1;
    JTextField costt1;
    JTextField amtt1;
    JScrollPane sp;
    JButton insert;
    JButton update;
    JButton delete;
    String column[]={"ID","DATE","REGION","PRODUCT","QUANTITY","COST","AMT"};
    BO2() throws IOException, TimeoutException {
        f = new JFrame();
        f.setSize(300,400);
        f.setVisible(true);
        deleted=new JLabel("Deleted Elements:");
        model=new DefaultTableModel(column,0);
        model2=new DefaultTableModel(column,0);
        model.addRow(column);
        model2.addRow(column);
        jt=new JTable(model);
        jt2=new JTable(model2);
        f.add(jt);
        panel1=new JPanel();
        panel2=new JPanel();
        panel3=new JPanel();
        panel4=new JPanel();
        panel5=new JPanel();
        id2=new JLabel("id:");
        rollback=new JButton("rollback");
        submit1=new JButton("insert");
        submit2=new JButton("update");
        submit3=new JButton("delete");
        submit3.setPreferredSize(new Dimension(200,24));
        submit4=new JButton("rollback");
        submit4.setPreferredSize(new Dimension(200,24));
        id=new JLabel("id:");
        date=new JLabel("date:");
        region=new JLabel("region:");
        product=new JLabel("product:");
        qty=new JLabel("qty:");
        cost=new JLabel("cost:");
        amt=new JLabel("amt:");
        date1=new JLabel("date:");
        region1=new JLabel("region:");
        product1=new JLabel("product:");
        qty1=new JLabel("qty:");
        cost1=new JLabel("cost:");
        amt1=new JLabel("amt:");
        idt=new JTextField();
        idt.setPreferredSize( new Dimension( 100, 24 ) );
        id1=new JLabel("id:");
        idt1=new JTextField();
        idt1.setPreferredSize( new Dimension( 500, 24 ) );
        idt2=new JTextField();
        idt2.setPreferredSize( new Dimension( 600, 24 ) );
        datet=new JTextField();
        datet.setPreferredSize( new Dimension( 100, 24 ) );
        regiont=new JTextField();
        regiont.setPreferredSize( new Dimension( 100, 24 ) );
        productt=new JTextField();
        productt.setPreferredSize( new Dimension( 100, 24 ) );
        qtyt=new JTextField();
        qtyt.setPreferredSize( new Dimension( 100, 24 ) );
        costt=new JTextField();
        costt.setPreferredSize( new Dimension( 100, 24 ) );
        amtt=new JTextField();
        amtt.setPreferredSize( new Dimension( 100, 24 ) );
        datet1=new JTextField();
        datet1.setPreferredSize( new Dimension( 120, 24 ) );
        regiont1=new JTextField();
        regiont1.setPreferredSize( new Dimension( 120, 24 ) );
        productt1=new JTextField();
        productt1.setPreferredSize( new Dimension( 120, 24 ) );
        qtyt1=new JTextField();
        qtyt1.setPreferredSize( new Dimension( 120, 24 ) );
        costt1=new JTextField();
        costt1.setPreferredSize( new Dimension( 120, 24 ) );
        amtt1=new JTextField();
        amtt1.setPreferredSize( new Dimension( 120, 24 ) );
        panel1.add(date1);
        panel1.add(datet1);
        panel1.add(region1);
        panel1.add(regiont1);
        panel1.add(product1);
        panel1.add(productt1);
        panel1.add(qty1);
        panel1.add(qtyt1);
        panel1.add(cost1);
        panel1.add(costt1);
        panel1.add(amt1);
        panel1.add(amtt1);
        panel1.add(submit1);
        panel1.setVisible(false);
        panel2.add(id);
        panel2.add(idt);
        panel2.add(date);
        panel2.add(datet);
        panel2.add(region);
        panel2.add(regiont);
        panel2.add(product);
        panel2.add(productt);
        panel2.add(qty);
        panel2.add(qtyt);
        panel2.add(cost);
        panel2.add(costt);
        panel2.add(amt);
        panel2.add(amtt);
        panel2.add(submit2);
        panel2.setVisible(false);
        panel3.add(id1);
        panel3.add(idt1);
        panel3.add(submit3);
        panel3.setVisible(false);
        panel4.add(deleted);
        panel4.add(jt2);
        panel5.add(id2);
        panel5.add(idt2);
        panel5.add(submit4);
        panel4.setVisible(false);
        panel5.setVisible(false);
        f.setLayout(new FlowLayout());
        this.connection();
        basicConsume();
        try {
            this.afficher();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        this.insert=new JButton("insert");
        this.update=new JButton("update");
        this.delete=new JButton("delete");
        insert.addActionListener(this);
        update.addActionListener(this);
        delete.addActionListener(this);
        rollback.addActionListener(this);
        submit1.addActionListener(this);
        submit2.addActionListener(this);
        submit3.addActionListener(this);
        this.insert.setBackground(new Color(0,128,0));
        this.update.setBackground(new Color(255,165,0));
        this.delete.setBackground(new Color(255,0,0));
        this.insert.setSize(new Dimension(100,100));
        this.update.setSize(new Dimension(25,10));
        this.delete.setSize(new Dimension(25,10));
        this.insert.setVisible(true);
        f.add(insert);
        f.add(update);
        f.add(delete);
        f.add(rollback);
        f.add(panel1);
        f.add(panel2);
        f.add(panel3);
        f.add(panel4);
        f.add(panel5);
    }
    @Override
    public void actionPerformed(ActionEvent e){
        if(e.getSource()==insert){
            panel1.setVisible(true);
            panel2.setVisible(false);
            panel3.setVisible(false);
            panel4.setVisible(false);
            panel5.setVisible(false);


            System.out.println("insert");
        }
        if(e.getSource()==update){
            panel1.setVisible(false);
            panel2.setVisible(true);
            panel3.setVisible(false);
            panel4.setVisible(false);
            panel5.setVisible(false);

            System.out.println("update");
        }
        if(e.getSource()==rollback){
            panel1.setVisible(false);
            panel2.setVisible(false);
            panel3.setVisible(false);
            panel4.setVisible(true);
            panel5.setVisible(true);

            System.out.println("rollback");
        }
        if(e.getSource()==delete){
            panel1.setVisible(false);
            panel2.setVisible(false);
            panel3.setVisible(true);
            panel4.setVisible(false);
            panel5.setVisible(false);


            System.out.println("delete");
        }
        if(e.getSource()==submit1){
            JSONObject json=new JSONObject();
            json.put("date",datet1.getText());
            json.put("product",productt1.getText());
            json.put("region",regiont1.getText());
            json.put("qty",qtyt1.getText());
            json.put("cost",costt1.getText());
            json.put("amt",amtt1.getText());
            this.insert(json);
            json.put("operation","insert");
            try{
                this.afficher();
                EmitLog(json,"bo2-ho");
            }
            catch (SQLException | IOException r){
                r.printStackTrace();
            }
        }
        if(e.getSource()==submit2){
            JSONObject json=new JSONObject();
            json.put("id",idt.getText());
            json.put("date",datet.getText());
            json.put("product",productt.getText());
            json.put("region",regiont.getText());
            json.put("qty",qtyt.getText());
            json.put("cost",costt.getText());
            json.put("amt",amtt.getText());
            this.update(json);
            json.put("operation","update");
            try{
                this.afficher();
                EmitLog(json,"bo2-ho");
            }
            catch (SQLException | IOException r){
                r.printStackTrace();
            }
        }
        if(e.getSource()==submit3){
            JSONObject json=new JSONObject();
            json.put("id",idt1.getText());
            this.delete(json);
            json.put("operation","delete");
            try{
                this.afficher();
                EmitLog(json,"bo2-ho");
            }
            catch (SQLException | IOException r){
                r.printStackTrace();
            }
        }
        if(e.getSource()==submit4){
            JSONObject json=new JSONObject();
            json.put("id",idt2.getText());
            this.rollback(json);
            json.put("operation","rollback");
            try{
                this.afficher();
                EmitLog(json,"bo2-ho");
            }
            catch (SQLException | IOException r){
                r.printStackTrace();
            }
        }
    }

    private void rollback(JSONObject json) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("UPDATE `product sales` set `deletedAt`=null where id='"+json.get("id")+"'");
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }

    public void afficher() throws SQLException {
        ResultSet results=this.retrieve();
        ResultSet results2=this.retrieve2();
        model=new DefaultTableModel(column,0);
        model.addRow(column);
        model2=new DefaultTableModel(column,0);
        model2.addRow(column);
        String data[]=new String[7];
        String data2[]=new String[7];
        try {
            while (results.next()) {
                for(int j=1;j<8;j++) {
                    String row = results.getString(j);
                    data[j-1]=row;
                }
                model.addRow(data);
            }
            jt.setModel(model);
            while (results2.next()) {
                for(int j=1;j<8;j++) {
                    String row = results2.getString(j);
                    data2[j-1]=row;
                }
                model2.addRow(data2);
            }
            jt2.setModel(model2);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
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
    public ResultSet retrieve2(){
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT id ,date,Region,Product,Qty,Cost,Amt FROM `product sales` where deletedAt is not null");
            return results;
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
        return null;
    }
    public void update(JSONObject json){
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("UPDATE `product sales` set `Date`='"+json.get("date")+"',`Region`='"+json.get("region")+"',`Product`='"+json.get("product")+"', `Qty`='"+json.get("qty")+"', `Cost`='"+json.get("cost")+"', `Amt`='"+json.get("amt")+"' Where id='"+json.get("id")+"'");
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }

    public void insert(JSONObject json){
        try {
            Statement statement = connection.createStatement();
            String sql="INSERT INTO `product sales`(`Date`, `Region`, `Product`, `Qty`, `Cost`, `Amt`) VALUES('"+json.get("date")+"','"+json.get("region")+"','"+json.get("product")+"',"+json.get("qty")+","+json.get("cost")+","+json.get("amt")+") ";
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void delete(JSONObject message){
        try {
            Statement statement = connection.createStatement();
            Date date=new Date();
            SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String date1=formater.format(date);
            statement.executeUpdate("Update `product sales` set deletedAt='"+date1 +"' WHERE id='"+message.get("id")+"'");

        } catch (SQLException e) {
            System.out.println("Could not retrieve data from the database " + e.getMessage());
        }
    }
    public void connection(){
        try {
            String driverName = "com.mysql.jdbc.Driver";
            Class.forName(driverName);
            String serverName = "localhost";
            String schema = "bo2";
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
        channel.queueBind(queueName, "exchange", "ho-bo2");
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
                else if(json.get("operation").equals("rollback")){rollback(json);}
                afficher();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
    public void EmitLog(JSONObject obj, String Queue_name) throws IOException {
        obj.put("sender","bo2");
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

    public static void main(String[] args) throws  Exception{
        BO2 bo2=new BO2();
    }


}
