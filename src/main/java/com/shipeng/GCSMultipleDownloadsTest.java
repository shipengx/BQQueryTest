package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GCSMultipleDownloadsTest {

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

        /*

        Blob blob = storage.get(BlobId.of("shipeng_test", "cli_test.csv"));

        // The path to which the file should be downloaded
        Path destFilePath = Paths.get("/Users/sxu/test/cli_test.csv");

        // Download file to specified path
        blob.downloadTo(destFilePath);
        */

        Bucket bucket = storage.get("shipeng_test");
        if (bucket == null) {
            System.out.println("No such bucket");
            return;
        }

        for (Blob blob : bucket.list().iterateAll()) {
            System.out.println(blob.getName());

            // The path to which the file should be downloaded
            Path destFilePath = Paths.get("/opt/opinmind/var/cli/nzTmpDir/" + blob.getName());

            // Download file to specified path
            blob.downloadTo(destFilePath);

            boolean deleted = storage.delete(blob.getBlobId());
            if (deleted) {
                // the blob was deleted
                System.out.println("the blob with name " + blob.getName() + " is deleted. ");
            } else {
                // the blob was not found
                System.out.println("the blob was not found");
            }
        }

        System.out.println("download is done.");

    }

}
