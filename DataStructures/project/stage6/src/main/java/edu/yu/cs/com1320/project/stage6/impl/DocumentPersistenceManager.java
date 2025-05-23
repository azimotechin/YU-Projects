package edu.yu.cs.com1320.project.stage6.impl;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import jakarta.xml.bind.DatatypeConverter;
import edu.yu.cs.com1320.project.stage6.Document;
import edu.yu.cs.com1320.project.stage6.PersistenceManager;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.HashMap;

public class DocumentPersistenceManager implements PersistenceManager<URI, Document> {
    private final File baseDir;
    private final Gson gson;

    private static class DocumentAdapter implements JsonSerializer<Document>, JsonDeserializer<Document> {
        @Override
        public JsonElement serialize(Document doc, Type type, JsonSerializationContext context) {
            JsonObject jsonObject = JsonParser.parseString(new Gson().toJson(doc)).getAsJsonObject();
            if (doc.getDocumentTxt() != null) {
                jsonObject.add("wordMap", new Gson().toJsonTree(doc.getWordMap()));
            } else {
                String base64 = DatatypeConverter.printBase64Binary(doc.getDocumentBinaryData());
                jsonObject.addProperty("binaryData", base64);
            }
            return jsonObject;
        }

        @Override
        public Document deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonObject json = element.getAsJsonObject();
            URI uri = URI.create(json.get("uri").getAsString());
            long lastUseTime = System.nanoTime();
            HashMap<String, String> metadata = new Gson().fromJson(json.get("metadata"), new TypeToken<HashMap<String, String>>(){}.getType());
            Document doc;
            if (json.has("text")) {
                String text = json.get("text").getAsString();
                HashMap<String, Integer> wordMap = new Gson().fromJson(json.get("wordMap"), new TypeToken<HashMap<String, Integer>>(){}.getType());
                doc = new DocumentImpl(uri, text, wordMap);
            } else {
                byte[] binaryData = DatatypeConverter.parseBase64Binary(json.get("binaryData").getAsString());
                doc = new DocumentImpl(uri, binaryData);
            }
            doc.setMetadata(metadata);
            doc.setLastUseTime(lastUseTime);
            return doc;
        }

    }

    public DocumentPersistenceManager(File baseDir) {
        this.baseDir = (baseDir == null) ? new File(System.getProperty("user.dir")) : baseDir;
        this.gson = new GsonBuilder().registerTypeAdapter(Document.class, new DocumentAdapter()).create();
    }

    @Override
    public void serialize(URI uri, Document val) throws IOException {
        if (uri == null || val == null) {
            throw new IllegalArgumentException("URI or Document is null");
        }
        File file = getFileFromUri(uri);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Could not create directory: " + parent.getAbsolutePath());
        }

        try (Writer writer = new FileWriter(file)) {
            gson.toJson(val, Document.class, writer);
        }
    }

    @Override
    public Document deserialize(URI uri) throws IOException {
        if (uri == null) throw new IllegalArgumentException("URI is null");
        File file = getFileFromUri(uri);
        if (!file.exists()) {
            return null;
        }
        try (Reader reader = new FileReader(file)) {
            return gson.fromJson(reader, Document.class);
        }
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        if (uri == null) {
            throw new IllegalArgumentException("URI is null");
        }
        File file = getFileFromUri(uri);
        return file.delete();
    }

    private File getFileFromUri(URI uri) throws IOException {
        File file = getFile(uri);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IOException("Could not create directory: " + parentDir.getAbsolutePath());
            }
        }
        return file;
    }

    private File getFile(URI uri) {
        String host = uri.getHost();
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            path = "/index";
        }
        path = path.replaceFirst("^/", "");
        return new File(baseDir, host + File.separator + path + ".json");
    }
}