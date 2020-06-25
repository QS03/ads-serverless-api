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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class AsapDetailHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        String caseNumber = "";
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            caseNumber = queryStringParameters.get("asap");
        }

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
                JSONArray details = runQuery(connection, caseNumber);
                statusCode = 200;
                retObject.put("data", details);
            } else {
                statusCode = 501;
                data.put("message", "Server error!");
                retObject.put("data", data);
            }
        } catch (JSONException e) {
            LOG.info("Error: {}", e);
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Headers", "Content-Type, Origin, Access-Control-Allow-Headers");

        String response = "";
        try {
            response = retObject.toString(4);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return ApiGatewayResponse.builder()
                .setStatusCode(statusCode)
                .setRawBody(response)
                .setHeaders(headers)
                .build();
    }

    public JSONArray runQuery(Connection connection, String caseNumber) throws JSONException {

        String query = "--asap detail\n" +
                "select case_number, \"Date In\" \"actionTime\" , \"Assigned To Display Name\" \"user\",  \"Role\" \"role\", \"Step Display Name\" \"stepName\", \"Action Name\" \"actionName\"\n" +
                "FROM \"ADMIN\".\"sample_data_2\"\n" +
                "WHERE \"Date In\" IS NOT NULL\n";
        if(!caseNumber.equals(""))query += "AND case_number = '" + caseNumber + "' ORDER BY \"actionTime\" ASC";


        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray details = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while(rs.next()) {
                JSONObject item = new JSONObject();
                item.put("CASE_NUMBER", rs.getString("CASE_NUMBER"));
                item.put("actionTime", rs.getString("actionTime"));
                item.put("user", rs.getString("user"));
                item.put("role", rs.getString("role"));
                item.put("stepName", rs.getString("stepName"));
                item.put("actionName", rs.getString("actionName"));
                details.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        }
        return details;
    }
}
