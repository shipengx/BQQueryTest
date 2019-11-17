package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.UUID;


public class TestCopy {

    public static void main(String[] args) throws Exception {

        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-discovery-qa-3c8e78675857.json";
        GoogleCredentials credentials;
        File credentialsPath = new File(jsonKeyFile);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("adara-discovery-qa").build().getService();

        String datasetId = "sam_dev";
        String destinationTableId = "audience_data_copy";

        TableId destinationTable = TableId.of(datasetId, destinationTableId);
        CopyJobConfiguration configuration =
                CopyJobConfiguration.newBuilder(
                        destinationTable,
                        Arrays.asList(
                                TableId.of(datasetId, "audience_data")))
                        .build();
        CopyJobConfiguration.Builder builder = configuration.toBuilder();
        builder.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND);
        //builder.setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);
        builder.setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        configuration = builder.build();

        // Copy the tables.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job job = bigquery.create(JobInfo.of(jobId, configuration));
        job = job.waitFor();

        // Check the table
        StandardTableDefinition table = bigquery.getTable(destinationTable).getDefinition();
        System.out.println("State: " + job.getStatus().getState());
        System.out.printf("Copied %d rows.\n", table.getNumRows());

        // Check for errors
        if (job == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (job.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(job.getStatus().getError().toString());
        }
    }

}
