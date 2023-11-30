package com.mongodb.developers;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class ResumableChangeStream {
    private static final String DATABASE_NAME = "test";
    private static final String COLLECTION_NAME = "foo";
    private static final String RESUME_COLLECTION_NAME = COLLECTION_NAME + "_resumeToken";
    private static final int CAP_SIZE = 16 * 1024 * 1024; // 16 MB

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private MongoCollection<Document> resumeCollection;

    public ResumableChangeStream() {
        mongoClient = MongoClients.create(MongoClientSettings.
                builder()
                .applicationName("ResumableChangeStream")
                        .applyConnectionString(new ConnectionString("mongodb+srv://ilian:Password.@m0.takskf9.mongodb.net/?"))
                .build()); 
        database = mongoClient.getDatabase(DATABASE_NAME);
        collection = database.getCollection(COLLECTION_NAME);
        initializeResumeCollection();
    }

    private void initializeResumeCollection() {
        try {
            // Create a capped collection with a size of 16MB and max 1 document
            CreateCollectionOptions options = new CreateCollectionOptions()
                    .capped(true)
                    .sizeInBytes(CAP_SIZE)
                    .maxDocuments(1);
            database.createCollection(RESUME_COLLECTION_NAME, options);
        } catch (MongoException e) {
            // Collection already exists
        }
        resumeCollection = database.getCollection(RESUME_COLLECTION_NAME);
    }

    public void startListening() {
        BsonDocument resumeToken = getLastResumeToken();

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.in("operationType", Arrays.asList("insert", "update"))));

        ChangeStreamIterable<Document> changeStream = collection.watch(pipeline)
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        if (resumeToken != null) {
            changeStream.resumeAfter(resumeToken);
        }

        try (MongoCursor<ChangeStreamDocument<Document>> cursor = changeStream.iterator()) {
            while (cursor.hasNext()) {
                ChangeStreamDocument<Document> change = cursor.next();
                processChange(change);
                updateResumeToken(change.getResumeToken());
            }
        }
    }

    private BsonDocument getLastResumeToken() {
        Document lastTokenDoc = resumeCollection.find().first();
        if (lastTokenDoc != null) {
            String tokenString = lastTokenDoc.getString("_id");
            try {
                // Decode the base64 string back to a byte array
                byte[] bsonBytes = Base64.getDecoder().decode(tokenString);
                return new RawBsonDocument(bsonBytes);
            } catch (IllegalArgumentException e) {
                // Handle invalid base64 string
                e.printStackTrace();
            }
        }
        return null;
    }

    private void processChange(ChangeStreamDocument<Document> change) {
        // Implement your logic to process the change event
        System.out.println("Received change: " + change);
    }



    private void updateResumeToken(BsonDocument resumeToken) {
        // Create a BsonDocumentCodec for serialization
        BsonDocumentCodec codec = new BsonDocumentCodec();
        BasicOutputBuffer buffer = new BasicOutputBuffer();

        codec.encode(new BsonBinaryWriter(buffer), resumeToken, EncoderContext.builder().build());
        byte[] bsonBytes = buffer.toByteArray();
        String encodedToken = Base64.getEncoder().encodeToString(bsonBytes);

        Document tokenDoc = new Document("_id", encodedToken);
        resumeCollection.insertOne(tokenDoc); // Store the encoded token
    }



    public static void main(String[] args) {
        ResumableChangeStream listener = new ResumableChangeStream();
        listener.startListening();
    }
}
