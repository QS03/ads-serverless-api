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
import java.util.Map;
import java.util.Optional;


public class RoleDurationsHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

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


            String query = "SELECT\n" +
                    "\t\"Role\" AS \"name\",\n" +
                    "\tAVG(\"Cycle Time - Days\") AS \"averageDuration\",\n" +
                    "\tMIN(\"Cycle Time - Days\") AS \"min\",\n" +
                    "\tMAX(\"Cycle Time - Days\") AS \"max\",\n" +
                    "\tCASE WHEN \"Role\" = 'Role 2' THEN\n" +
                    "\t\t3\n" +
                    "\tWHEN \"Role\" = 'Role 3' THEN\n" +
                    "\t\t2\n" +
                    "\tWHEN \"Role\" = 'Role 4' THEN\n" +
                    "\t\t3\n" +
                    "\tWHEN \"Role\" = 'Role 5' THEN\n" +
                    "\t\t1\n" +
                    "\tEND AS \"standardMin\",\n" +
                    "\tCASE WHEN \"Role\" = 'Role 2' THEN\n" +
                    "\t\t9\n" +
                    "\tWHEN \"Role\" = 'Role 3' THEN\n" +
                    "\t\t4\n" +
                    "\tWHEN \"Role\" = 'Role 4' THEN\n" +
                    "\t\t6\n" +
                    "\tWHEN \"Role\" = 'Role 5' THEN\n" +
                    "\t\t3\n" +
                    "\tEND AS \"standardMax\"\n" +
                    "FROM (\n" +
                    "\tSELECT\n" +
                    "\t\tCASE_NUMBER,\n" +
                    "\t\t\"Role\",\n" +
                    "\t\t\"ASAP CREATED\",\n" +
                    "\t\tCASE WHEN \"Date Out\" IS NULL\n" +
                    "\t\t\tAND \"Date In\" IS NOT NULL THEN\n" +
                    "\t\t\ttrunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\", 2)\n" +
                    "\t\tELSE\n" +
                    "\t\t\tCAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                    "\t\tEND AS \"Cycle Time - Days\",\n" +
                    "\t\tCASE WHEN \"Date Out\" IS NULL\n" +
                    "\t\t\tAND \"Date In\" IS NOT NULL THEN\n" +
                    "\t\t\ttrunc((cast(CURRENT_TIMESTAMP AS date) - \"Date In\") * 24, 2)\n" +
                    "\t\tELSE\n" +
                    "\t\t\tCAST(\"Cycle Time - Hours\" AS NUMBER)\n" +
                    "\t\tEND AS \"Cycle Time - Hours\",\n" +
                    "\t\t\"ASAP Status\"\n" +
                    "\tFROM\n" +
                    "\t\tADMIN. \"sample_data_2\"\n" +
                    "\tWHERE\n" +
                    "\t\t\"Role\" IS NOT NULL\n" +
                    "\t\tAND CASE WHEN \"Date Out\" IS NULL\n" +
                    "\t\t\tAND \"Date In\" IS NOT NULL THEN\n" +
                    "\t\t\ttrunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\", 2)\n" +
                    "\t\tELSE\n" +
                    "\t\t\tCAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                    "\t\tEND < 10000)\n" +
                    "WHERE\n" +
                    String.format("\t\"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                    String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate) +
                    "GROUP BY\n" +
                    "\t\"Role\"";

            LOG.info("Query: {}", query);

            try {
                if (Optional.ofNullable(connection).isPresent()) {
                    statusCode = 200;
                    retObject = runQuery(connection, query);
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

    /**
     * This method will run sample query against Oracle Database.
     *
     * @param connection
     * @param query
     */
    public JSONObject runQuery(Connection connection, String query) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONObject result = new JSONObject();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            JSONArray result_array = new JSONArray();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("name", rs.getString("name"));
                item.put("averageDuration", rs.getFloat("averageDuration"));
                item.put("min", rs.getFloat("min"));
                item.put("max", rs.getFloat("max"));
                item.put("max", rs.getFloat("max"));
                item.put("standardMin", rs.getFloat("standardMin"));
                item.put("standardMax", rs.getFloat("standardMax"));

                result_array.put(item);
            }
            LOG.info("Counts: {}", result_array.length());
            result.put("data", result_array);
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        return result;
    }
}
