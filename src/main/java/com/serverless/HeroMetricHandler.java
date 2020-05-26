package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class HeroMetricHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        String startDate = null;
        String endDate = null;
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            startDate = queryStringParameters.get("start");
            endDate = queryStringParameters.get("end");
        }

        if(startDate == null)startDate = "1900-01-01";
        if(endDate == null)endDate = "2100-01-01";

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        if( Validator.isValidateDate(startDate) && Validator.isValidateDate(endDate)) {
            DBCredentials dbCreds = new DBCredentials();
            dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
            dbCreds.setDbPort("1521");
            dbCreds.setUserName("admin");
            dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
            dbCreds.setDbName("orcl");

            DBConnection dbConnection = new DBConnection();
            Connection connection = dbConnection.getConnection(dbCreds);

            try {
                if (Optional.ofNullable(connection).isPresent()) {

                    int totalASAPRequests = getTotalASAPRequests(connection, startDate, endDate);
                    data.put("Total ASAP Requests", totalASAPRequests);

                    int activeASAPRequests = getActiveASAPRequests(connection, startDate, endDate);
                    data.put("Active ASAP Requests", activeASAPRequests);

                    int completedASAPRequests = getCompletedASAPRequests(connection, startDate, endDate);
                    data.put("Completed ASAP Requests", completedASAPRequests);

                    float averageASAPCycleTime = getAverageASAPCycleTime(connection, startDate, endDate);
                    data.put("Average ASAP Cycle Time", averageASAPCycleTime);

                    float fastestASAPTotalCycleTime = getFastestASAPTotalCycleTime(connection, startDate, endDate);
                    data.put("Fastest ASAP Total Cycle Time", averageASAPCycleTime);
                    statusCode = 200;
                    retObject.put("data", data);

                } else {
                    statusCode = 501;
                    data.put("message", "Server error!");
                    retObject.put("data", data);
                }
            } catch (JSONException e) {
                LOG.info("Error: {}", e);
            }
        } else {
            statusCode = 400;
            try {
                data.put("message", "Invalid date format");
                retObject.put("data", data);
            } catch (JSONException e) {
                LOG.info("Error: {}", e);
            }
        }

        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(retObject.toString())
                .setHeaders(Collections.singletonMap("Content-Type", "application/json"))
                .setHeaders(Collections.singletonMap("Access-Control-Allow-Origin", "*"))
                .build();
    }

    public int getTotalASAPRequests(Connection connection, String startDate, String endDate) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        int result = 0;

        String query = "\nSELECT count(DISTINCT (CASE_NUMBER)) as \"Total ASAP Requests\"\n" +
                "FROM ADMIN. \"Adjusted_Data\"\n";

        if (!startDate.equals("") && !endDate.equals("")) {
            query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
        }
        else {
            if (!startDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            }

            if (!endDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
            }
        }

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            rs.next();
            result = rs.getInt("Total ASAP Requests");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public int getActiveASAPRequests(Connection connection, String startDate, String endDate) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        int result = 0;

        String query = "SELECT count(DISTINCT (CASE_NUMBER)) AS \"Active ASAP Requests\"\n" +
                "FROM ADMIN.\"Adjusted_Data\"\n" +
                "WHERE \"ASAP Status\" = 'Active'\n";

        if (!startDate.equals("")) {
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
        }

        if (!endDate.equals("")) {
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
        }

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            rs.next();
            result = rs.getInt("Active ASAP Requests");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public int getCompletedASAPRequests(Connection connection, String startDate, String endDate) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        int result = 0;

        String query = "\nSELECT count(DISTINCT (CASE_NUMBER)) as \"Completed ASAP Requests\"\n" +
                "FROM\n ADMIN.\"Adjusted_Data\"\n" +
                "WHERE \"ASAP Status\" = 'Completed'\n";

        if (!startDate.equals("")) {
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
        }

        if (!endDate.equals("")) {
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
        }

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            rs.next();
            result = rs.getInt("Completed ASAP Requests");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public float getAverageASAPCycleTime(Connection connection, String startDate, String endDate) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        float result = 0.0f;

        String query = "\nWITH cycle_time AS (\n" +
                "SELECT\n" +
                "MAX(\"ASAP Total Cycle Time\") AS \"ASAP Total Cycle Time\",\n" +
                "CASE_NUMBER\n" +
                "FROM\n" +
                "ADMIN.\"Adjusted_Data\"\n";

        if (!startDate.equals("") && !endDate.equals("")) {
            query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
        }
        else {
            if (!startDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            }

            if (!endDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
            }
        }

        query += "GROUP BY\n" +
                "CASE_NUMBER\n" +
                ")\n" +
                "SELECT\n" +
                "AVG(\"ASAP Total Cycle Time\") AS \"Average ASAP Cycle Time\"\n" +
                "FROM\n" +
                "cycle_time\n";

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            rs.next();
            result = rs.getFloat("Average ASAP Cycle Time");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public float getFastestASAPTotalCycleTime(Connection connection, String startDate, String endDate) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        float result = 0.0f;

        String query = "WITH cycle_time AS (\n" +
                "\tSELECT\n" +
                "\t\tMAX(\"ASAP Total Cycle Time\") AS \"ASAP Total Cycle Time\",\n" +
                "\t\tCASE_NUMBER,\n" +
                "\t\t\"ASAP Status\"\n" +
                "\tFROM\n" +
                "\t\tADMIN.\"Adjusted_Data\"\n";

        if (!startDate.equals("") && !endDate.equals("")) {
            query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            query += "AND TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
        }
        else {
            if (!startDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') >= TO_DATE('" + startDate + "','yyyy-MM-dd')\n";
            }

            if (!endDate.equals("")) {
                query += "WHERE TO_DATE(TO_NUMBER(TO_CHAR(\"ASAP Created\", 'YYYYDDMMHH24MISS')), 'YYYYMMDDHH24MISS') <= TO_DATE('" + endDate + "','yyyy-MM-dd')\n";
            }
        }

        query +="\tGROUP BY\n" +
                "\t\tCASE_NUMBER,\n" +
                "\t\t\"ASAP Status\"\n" +
                ")\n" +
                "SELECT\n" +
                "\tMIN(\"ASAP Total Cycle Time\") AS \"Fastest ASAP Total Cycle Time\"\n" +
                "FROM\n" +
                "\tcycle_time\n" +
                "WHERE\n" +
                "\t\"ASAP Status\" = 'Completed'\n" +
                "\tAND \"ASAP Total Cycle Time\" > 0";

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            rs.next();
            result = rs.getFloat("Fastest ASAP Total Cycle Time");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }
}
