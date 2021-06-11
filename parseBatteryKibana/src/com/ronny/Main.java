package com.ronny;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.elasticsearch.client.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;



import org.apache.http.HttpHost;

public class Main {

    public void updateDb(String source, float stateOfCharge, float powerConsumedFromStorage, float powerBuffered, String timeOffset,
                         String host, String port, int loggLevel) {
        SensorProperties sensor = new SensorProperties();
        if (!sensor.getSensorProperties(source))
            return;

        long sourceId = sensor.sourceId;
        String description = sensor.shortDescription;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss" + timeOffset.trim());
        LocalDateTime now = LocalDateTime.now();

        JSONObject jsonBody = new JSONObject();
        JSONObject jsonTags = new JSONObject();
        JSONObject jsonFields = new JSONObject();

        jsonTags.put("source", sourceId);
        jsonTags.put("source_desc", description);
        jsonFields.put("StateOfCharge", stateOfCharge);
        jsonFields.put("PowerConsumedFromStorage", powerConsumedFromStorage);
        jsonFields.put("PowerBuffered", powerBuffered);

        jsonBody.put("measurement", "BatteryLogg");
        jsonBody.put("date", dtf.format(now));
        jsonBody.put("tags", jsonTags);
        jsonBody.put("fields", jsonFields);

        if (loggLevel > 1)
            System.out.println(jsonBody);

        HttpHost esHost = new HttpHost(host, Integer.parseInt(port));
        RestClient restClient = RestClient.builder(esHost).build();

        Request request = new Request(
                "POST",
                "/battery_index/batterylog");

        request.setJsonEntity(jsonBody.toString());

        Response response = null;
        try {
            response = restClient.performRequest(request);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                restClient.close();
            } catch (IOException closeEx){
                closeEx.printStackTrace();
            }

            if (loggLevel > 0)
                System.out.println(response);

        }
    }


    public void run() {
        System.out.println("TopicSubscriber initializing...");
        System.out.println("Get properties from file...");

        String host = "";
        String username = "";
        String password = "";
        String topic = "";
        String timeOffset = "";
        String kibanaHost = "";
        String kibanaPort = "";
        int loggLevel = 0;


        try (InputStream input = new FileInputStream("parseBatteryKibana.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            host = prop.getProperty("mqttHost");
            username = prop.getProperty("mqttUsername");
            password = prop.getProperty("mqttPassword");
            topic = prop.getProperty("mqttTopic");
            timeOffset = prop.getProperty("timeOffset");
            kibanaHost = prop.getProperty("kibanaHost");
            kibanaPort = prop.getProperty("kibanaPort");
            loggLevel = Integer.parseInt( prop.getProperty("loggLevel"));
            System.out.println("Logglevel: "+ loggLevel);

        } catch (IOException ex) {
            ex.printStackTrace();
        }



        try {
            // Create an Mqtt client
            Random rand = new Random();
            MqttClient mqttClient = new MqttClient(host, "parseBattery" + rand.toString());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
            connOpts.setAutomaticReconnect(true);

            // Connect the client
            System.out.println("Connecting to Kjuladata messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Topic filter the client will subscribe to
            final String subTopic = topic;

            // Callback - Anonymous inner-class for receiving messages
            String finalTimeOffset = timeOffset;
            String finalKibanaHost = kibanaHost;
            String finalKibanaPort = kibanaPort;
            int finalLoggLevel = loggLevel;
            mqttClient.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server that
                    // matches any subscription made by the client

                    JSONParser parser = new JSONParser();
                    String payLoad = new String(message.getPayload());

                    if (finalLoggLevel > 1)
                        System.out.println(payLoad);
                    JSONObject json = (JSONObject) parser.parse(payLoad);
                    JSONArray environmentlogg = (JSONArray) json.get("emlogg");
                    try {
//                        FileWriter jsonFile = new FileWriter("/Users/f2530720/IdeaProjects/parseEnvironmentKibana/tmp/parseEnvironmentKibana_java.json", false);
                        FileWriter jsonFile = new FileWriter("/tmp/energy.json", false);

                        jsonFile.write(payLoad + "\n");
                        jsonFile.close();
                    } catch (IOException e) {
                        System.err.print("FileWriter went wrong");
                        e.printStackTrace();
                    }

                    try {
                        for (Object logg : environmentlogg) {
                            JSONObject loggbysource = (JSONObject) logg;
                            String source = "12";
                            float stateOfCharge = ((Number)loggbysource.get("StateOfCharge")).floatValue();
                            float powerConsumedFromStorage = ((Number) loggbysource.get("PowerConsumedFromStorage")).floatValue();
                            float powerBuffered = ((Number) loggbysource.get("PowerBuffered")).floatValue();

                            updateDb(source, stateOfCharge, powerConsumedFromStorage, powerBuffered, finalTimeOffset,
                                    finalKibanaHost, finalKibanaPort, finalLoggLevel);
                        }
                    } catch (Exception pe) {
                        //  System.out.println("position: " + pe.getPosition());
                        System.out.println(pe);
                    }
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to KjulaData messaging lost!" + cause.getMessage());
                    System.exit(0);
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }

            });

            // Subscribe client to the topic filter and a QoS level of 0
            System.out.println("Subscribing client to topic: " + subTopic);
            mqttClient.subscribe(subTopic, 0);
            System.out.println("Subscribed");

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main().run();

    }
}
