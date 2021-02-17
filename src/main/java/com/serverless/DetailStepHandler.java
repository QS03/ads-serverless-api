package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class DetailStepHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        String startDate = null;
        String endDate = null;
        String caseNumber = "";
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            startDate = queryStringParameters.get("start");
            endDate = queryStringParameters.get("end");
            caseNumber = queryStringParameters.get("asap");
        }

        if (startDate == null) startDate = "1900-01-01";
        if (endDate == null) endDate = "2100-01-01";

            if (Validator.isValidateDate(startDate) && Validator.isValidateDate(endDate)) {
                DBCredentials dbCreds = new DBCredentials();
                dbCreds.setDbHost("covid-oracle.xxxxxxx.us-west-2.rds.amazonaws.com");
                dbCreds.setDbPort("1521");
                dbCreds.setUserName("admin");
                dbCreds.setPassword("xxxxxxx");
                dbCreds.setDbName("orcl");

                DBConnection dbConnection = new DBConnection();
                Connection connection = dbConnection.getConnection(dbCreds);


                try {
                    if (Optional.ofNullable(connection).isPresent()) {
                        statusCode = 200;
                        JSONArray detailSteps = getDetailSteps(connection, startDate, endDate, caseNumber);
                        data.put("detailSteps", detailSteps);
                    } else {
                        statusCode = 501;
                        data.put("message", "Server error!");
                    }

                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            } else {
                statusCode = 400;
                try {
                    data.put("message", "Invalid date format");
                } catch (JSONException e) {
                    LOG.info("Error: {}", e);
                }
            }


        try {
            retObject.put("data", data);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Headers", "Content-Type, Origin, Access-Control-Allow-Headers");


        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(retObject.toString())
                .setHeaders(headers)
                .build();
    }

    JSONArray getDetailSteps(Connection connection, String startDate, String endDate,String caseNumber) {

        String query = "--For Step Average, min, max\n" +
                "WITH stepdurations as ( SELECT\n" +
                "\t\"Step Display Name\" AS \"name\",\n" +
                "\tAVG(\"Cycle Time - Days\") AS \"averageDuration\",\n" +
                "\tMIN(\"Cycle Time - Days\") AS \"min\",\n" +
                "\tMAX(\"Cycle Time - Days\") AS \"max\",\n" +
                "\tCASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 3' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 4' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 5' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 6' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 7' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 8' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 9' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 10' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 12' THEN 1\n" +
                "\tEND AS \"standardMin\",\n" +
                "\tCASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 3' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 4' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 5' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 6' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 7' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 8' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 9' THEN 3\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 10' THEN 1\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 12' THEN 3\n" +
                "\tEND AS \"standardMax\"\n" +
                "FROM (\n" +
                "\tSELECT\n" +
                "\t\tCASE_NUMBER, \"ASAP CREATED\", \"Step Display Name\", \"Date In\", \"Date Out\",\n" +
                "\t\tCASE WHEN \"Date Out\" IS NULL AND \"Date In\" IS NOT NULL THEN trunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\", 2)\n" +
                "\t\tELSE CAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                "\t\tEND AS \"Cycle Time - Days\",\n" +
                "\t\tCASE WHEN \"Date Out\" IS NULL AND \"Date In\" IS NOT NULL THEN trunc((cast(CURRENT_TIMESTAMP AS date) - \"Date In\") * 24, 2)\n" +
                "\t\tELSE CAST(\"Cycle Time - Hours\" AS NUMBER)\n" +
                "\t\tEND AS \"Cycle Time - Hours\", \n" +
                "\t\t\"ASAP Status\"\n" +
                "\tFROM ADMIN. \"sample_data_2\" \n" +
                "\tWHERE\n" +
                "\t\tCASE WHEN \"Date Out\" IS NULL AND \"Date In\" IS NOT NULL THEN trunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\", 2)\n" +
                "\t\tELSE CAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                "\t\tEND < 1000) a\n" +
                "WHERE\n" +
                "\t\"Step Display Name\" IS NOT NULL\n" +
                String.format("\tAND \"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate) +
                "GROUP BY \"Step Display Name\"\n" +
                "ORDER BY \"Step Display Name\" ASC),\n" +
                "detailrole as (SELECT case_number AS \"asap\", \"Step Display Name\" AS \"name\", sum(\"Cycle Time - Days\") AS \"avgStepTime\"\n" +
                "\tFROM \"ADMIN\".\"sample_data_2\"\n" +
                "\tWHERE \"Cycle Time - Days\" IS NOT NULL\n";
                if(!caseNumber.equals(""))query += "AND case_number = '"+ caseNumber + "'\n";
                query += "\tGROUP BY CASE_NUMBER, \"Step Display Name\")\n" +
                "select a.*, b.\"avgStepTime\" from stepdurations a left join detailrole b on a.\"name\" = b.\"name\"";

        LOG.info("getDetailSteps query: {}", query);


        JSONArray detailSteps = new JSONArray();
        try {
            PreparedStatement prepStmt = connection.prepareStatement(query);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("name", rs.getString("name"));
                item.put("averageDuration", rs.getFloat("averageDuration"));
                item.put("min", rs.getFloat("min"));
                item.put("max", rs.getFloat("max"));
                item.put("standardMin", rs.getFloat("standardMin"));
                item.put("standardMax", rs.getFloat("standardMax"));
                item.put("avgTime", rs.getFloat("avgStepTime"));
                detailSteps.put(item);
            }
            LOG.info("Counts: {}", detailSteps.length());
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        return detailSteps;
    }
}
