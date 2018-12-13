package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import org.threeten.bp.Duration;

import java.io.File;
import java.io.FileInputStream;

import java.util.List;
import java.util.ArrayList;

public class BQToGCSTest {

    public static void main(String[] args) throws Exception {

        String destinationDataset = "sam_test";
        String destinationTable = "cli_test_query";
        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-machinelearning-371a3b1781df.json";
        GoogleCredentials credentials;
        File credentialsPath = new File(jsonKeyFile);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        BigQuery bq = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("adara-machinelearning").build().getService();
        Table table = bq.getTable(destinationDataset, destinationTable);
        //extractSingle(table, "CSV", "gs://shipeng_test/cli_test.csv");
        extractList(table,"CSV", "gs://shipeng_test/cli_test_*.csv", null);

    }


    /**
     * Example extracting data to single Google Cloud Storage file.
     */
    // [TARGET extract(String, String, JobOption...)]
    // [VARIABLE "CSV"]
    // [VARIABLE "gs://my_bucket/filename.csv"]
    public static Job extractSingle(Table table, String format, String gcsUrl) {
        // [START bigquery_extract_table]
        Job job = table.extract(format, gcsUrl);
        // Wait for the job to complete
        try {
            Job completedJob =
                    job.waitFor(
                            RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                            RetryOption.totalTimeout(Duration.ofMinutes(3)));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                // Job completed successfully
                System.out.println("extract table records is done.");
            } else {
                // Handle error case
            }
        } catch (InterruptedException e) {
            // Handle interrupted wait
            System.out.println("Exception occurred. ");
        }
        // [END bigquery_extract_table]
        return job;
    }


    /**
     * Example of partitioning data to a list of Google Cloud Storage files.
     */
    // [TARGET extract(String, List, JobOption...)]
    // [VARIABLE "CSV"]
    // [VARIABLE "gs://my_bucket/PartitionA_*.csv"]
    // [VARIABLE "gs://my_bucket/PartitionB_*.csv"]
    public static Job extractList(Table table, String format, String gcsUrl1, String gcsUrl2) {
        // [START ]
        List<String> destinationUris = new ArrayList<>();
        destinationUris.add(gcsUrl1);
        //destinationUris.add(gcsUrl2);
        Job job = table.extract(format, destinationUris);
        // Wait for the job to complete
        try {
            Job completedJob =
                    job.waitFor(
                            RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                            RetryOption.totalTimeout(Duration.ofMinutes(3)));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                // Job completed successfully
            } else {
                // Handle error case
            }
        } catch (InterruptedException e) {
            // Handle interrupted wait
        }
        // [END ]
        return job;
    }



}
