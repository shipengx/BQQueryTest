package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;

public class TestGetCount {

    public static void main(String[] args) throws Exception {


        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-discovery-qa-3c8e78675857.json";
        GoogleCredentials credentials;
        File credentialsPath = new File(jsonKeyFile);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        String query = " INSERT INTO `adara-discovery-qa.tmp_discovery_dev.tmp_audience_data`\n" +
                " SELECT audience_id, cookie_id, modification_ts FROM `adara-discovery-qa.tmp_discovery_dev.audience_group_1` ; ";


        BigQuery bq = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("adara-discovery-qa").build().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        query)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // [END bigquery_simple_app_query]


        TableResult tableResult = queryJob.getQueryResults();
        long count = tableResult.getTotalRows();
        System.out.println("the number of rows is : " + count);


    }

}
