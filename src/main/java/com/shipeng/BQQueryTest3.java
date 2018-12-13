package com.shipeng;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;

public class BQQueryTest3 {

    public static void main(String[] args) throws Exception {
        String jsonKeyFile = "/opt/opinmind/conf/credentials/adara-machinelearning-371a3b1781df.json";
        GoogleCredentials credentials;
        File credentialsPath = new File(jsonKeyFile);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        BigQuery bq = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("adara-machinelearning").build().getService();
        TableResult tableResult = tableResult = listTableData(bq, "sam_test", "cli_test_query");

        int count = 0;

        while (tableResult.hasNextPage()) {

            for (FieldValueList row : tableResult.getValues()) {
                // do something with the row
                long cookieId = row.get("cookie_id").getLongValue();
                String deviceId = row.get("id_value").getStringValue();
                System.out.println(cookieId + "," + deviceId);

                count++;
                System.out.printf("\n");
            } // end for loop

        } // end while loop
        System.out.println("the number of rows is : " + count);

    }

    public static TableResult listTableData(BigQuery bigQuery, String datasetName, String tableName) {
        // [START ]
        // This example reads the result 100 rows per RPC call. If there's no need to limit the number,
        // simply omit the option.
        Schema schema =
                Schema.of(
                        Field.of("cookie_id", LegacySQLTypeName.INTEGER),
                        Field.of("id_value", LegacySQLTypeName.STRING));
        TableResult tableData =
                bigQuery.listTableData(datasetName, tableName, schema
                        ,BigQuery.TableDataListOption.pageSize(10000));

        // [END ]
        return tableData;
    }

}
