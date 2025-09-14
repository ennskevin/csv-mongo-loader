package com.ennsko;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

public class App {

    public static void main( String[] args ) {

        String[] fieldsFromConfig = null;
        String[] types = null;

        // open data config csv
        Path dataConfigPath = Paths.get(args[0]);
        try (BufferedReader reader = Files.newBufferedReader(dataConfigPath)) {
            fieldsFromConfig = processLine(reader.readLine());
            types = processLine(reader.readLine());
        }
        catch(IOException e) {
            System.err.println(e.getMessage());
        }
        
        // Open csv
        Path csvPath = Paths.get(args[1]);
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String line = reader.readLine();
            String[] fields = processLine(line);
            if (!Arrays.equals(fields, fieldsFromConfig)) return;
            
            // Connect to db
            try (MongoClient mongoClient = MongoClients.create(Database.connection)) {
                MongoDatabase database = mongoClient.getDatabase("cs314");
                MongoCollection<Document> collection = database.getCollection("cities");
                
                // Batch and write
                List<Document> batch = new ArrayList<>();
                int batchCap = 1000;
                while((line = reader.readLine()) != null) {
                    String[] values = processLine(line);
                    batch.add(buildDoc(fields, types, values));
                    if (batch.size() == batchCap) {
                        collection.insertMany(batch);
                        batch.clear();
                    }                   
                }
                if (batch.size() > 0) {
                    collection.insertMany(batch);
                }
                createIndexes(collection, fields);
            }
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private static String[] processLine(String line) {
        String[] values = line.split(",");
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].replace("\"", "");
        }
        return values;
    }
    
    private static Document buildDoc(String[] fields, String[] types, String[] values) {
        Document doc = new Document();
        for (int i = 0; i < fields.length; i++) {
            Object value = parseValue(types[i], values[i]);
            doc.append(fields[i], value);
        }
        return doc;
    }

    
    private static Object parseValue(String type, String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            switch (type.toLowerCase()) {
                case "int":
                return Integer.parseInt(value);
                case "long":
                return Long.parseLong(value);
                case "double":
                return Double.parseDouble(value);
                case "boolean":
                return Boolean.parseBoolean(value);
                case "string":
                return value;
                default:
                return value;
            }
        }
        catch (NumberFormatException e) {
            return value;
        }
    }

    private static void createIndexes(MongoCollection<Document> collection, String[] fields) {
        for (int i = 0; i < fields.length; i++) {
            String key = fields[i];
            collection.createIndex(Indexes.ascending(key));
        }
    }
}
