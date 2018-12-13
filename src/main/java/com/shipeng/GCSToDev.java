package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GCSToDev {

    public static void main(String[] args) throws Exception {

        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-machinelearning-371a3b1781df.json";

        Storage storage = null;
        if (jsonKeyFile == null || jsonKeyFile.length() == 0) {
            storage = StorageOptions.getDefaultInstance().getService();
        } else {
            // Load credentials from JSON key file. If you can't set the
            // GOOGLE_APPLICATION_CREDENTIALS
            // environment variable, you can explicitly load the credentials file to
            // construct the credentials.
            GoogleCredentials credentials;
            File credentialsPath = new File(jsonKeyFile);
            try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
                credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            }
            storage = StorageOptions.newBuilder().setProjectId("adara-machinelearning").setCredentials(credentials).build().getService();
        }


        // get specific file from specified bucket

        Blob blob = storage.get(BlobId.of("shipeng_test", "cli_test.csv"));

        // The path to which the file should be downloaded
        Path destFilePath = Paths.get("/Users/sxu/test/cli_test.csv");

        // Download file to specified path
        blob.downloadTo(destFilePath);

        System.out.println("download is done.");


    } // end main

}
