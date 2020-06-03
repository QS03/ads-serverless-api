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
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;


public class ASAPInspectorHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("input: {}", input);

        String startDate = null;
        String endDate = null;
        String orgCode = null;
        Map<String, String> queryStringParameters = (Map<String, String>)input.get("queryStringParameters");
        if(queryStringParameters != null ){
            startDate = queryStringParameters.get("start");
            endDate = queryStringParameters.get("end");
            orgCode = queryStringParameters.get("orgCode");
        }

        if(startDate == null)startDate = "1900-01-01";
        if(endDate == null)endDate = "2100-01-01";
        if(orgCode == null)orgCode = "";

        int statusCode = 400;
        JSONObject retObject = new JSONObject();
        JSONObject data = new JSONObject();

        if(!orgCode.equals("")  && !orgCode.equals("organization 1") && !orgCode.equals("organization 2")){
            try {
                data.put("message", "Invalid Org Code");
                retObject.put("data", data);
            } catch (JSONException e) {
                LOG.info("Error: {}", e);
            }
        } else if( !Validator.isValidateDate(startDate) || !Validator.isValidateDate(endDate))
        {
            try {
                data.put("message", "Invalid date format");
                retObject.put("data", data);
            } catch (JSONException e) {
                LOG.info("Error: {}", e);
            }
        } else {
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
                    statusCode = 200;
                    JSONArray activityData = getactivityData(connection, startDate, endDate, orgCode);
                    JSONArray activityDataRows = cleanactivityData(activityData);
                    JSONArray activityLegend = getActivityLegend(connection);
                    data.put("activityLegend", activityLegend);
                    data.put("activityDataRows", activityDataRows);
                    retObject.put("data", data);
                } else {
                    statusCode = 501;
                    data.put("message", "Server error!");
                    retObject.put("data", data);
                }

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

    public JSONArray getactivityData(Connection connection, String startDate, String endDate, String orgCode) {

        String query = "WITH roles AS (\n" +
                "    SELECT\n" +
                "        CASE_NUMBER,\n" +
                "        \"Role\",\n" +
                "        \"ASAP CREATED\",\n" +
                "        \"ASAP Status\" AS \"isActive\",\n" +
                "        CASE WHEN \"Date Out\" IS NULL\n" +
                "            AND \"Date In\" IS NOT NULL THEN\n" +
                "            trunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\",\n" +
                "                2)\n" +
                "        ELSE\n" +
                "            CAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                "        END AS \"Cycle Time - Days\",\n" +
                "        CASE WHEN \"Date Out\" IS NULL\n" +
                "            AND \"Date In\" IS NOT NULL THEN\n" +
                "            trunc((cast(CURRENT_TIMESTAMP AS date) - \"Date In\") * 24,\n" +
                "            2)\n" +
                "        ELSE\n" +
                "            CAST(\"Cycle Time - Hours\" AS NUMBER)\n" +
                "        END AS \"Cycle Time - Hours\",\n" +
                "        \"ASAP Status\",\n" +
                "        \"Org Code\"\n" +
                "    FROM\n" +
                "        ADMIN. \"sample_data_2\"\n" +
                "),\n" +
                "steps AS (\n" +
                "    SELECT\n" +
                "        CASE_NUMBER,\n" +
                "        \"Step Display Name\",\n" +
                "        \"ASAP CREATED\",\n" +
                "        \"ASAP Status\" AS \"isActive\",\n" +
                "        CASE WHEN \"Date Out\" IS NULL\n" +
                "            AND \"Date In\" IS NOT NULL THEN\n" +
                "            trunc(cast(CURRENT_TIMESTAMP AS date) - \"Date In\",\n" +
                "                2)\n" +
                "        ELSE\n" +
                "            CAST(\"Cycle Time - Days\" AS NUMBER)\n" +
                "        END AS \"Cycle Time - Days\",\n" +
                "        CASE WHEN \"Date Out\" IS NULL\n" +
                "            AND \"Date In\" IS NOT NULL THEN\n" +
                "            trunc((cast(CURRENT_TIMESTAMP AS date) - \"Date In\") * 24,\n" +
                "            2)\n" +
                "        ELSE\n" +
                "            CAST(\"Cycle Time - Hours\" AS NUMBER)\n" +
                "        END AS \"Cycle Time - Hours\",\n" +
                "        \"ASAP Status\",\n" +
                "        \"Org Code\"\n" +
                "    FROM\n" +
                "        ADMIN. \"sample_data_2\"\n" +
                ")\n" +
                ", combined as (\n" +
                "SELECT\n" +
                "    \"isActive\",\n" +
                "    \"totalTime\",\n" +
                "    12 AS \"totalMin\",\n" +
                "    24 AS \"totalMax\",\n" +
                "    a. \"CASE_NUMBER\" AS \"asap\",\n" +
                "    Cast(SUBSTR(a.CASE_NUMBER, 8,LENGTH(a.CASE_NUMBER)-7)as NUMBER) as num,\n" +
                "    \"Role\" AS \"name\",\n" +
                "    CASE WHEN \"Role\" = 'Role 1' then '#B7B7C5'\n" +
                "    WHEN \"Role\" = 'Role 2' then '#D2D2D2'\n" +
                "    WHEN \"Role\" = 'Role 3' then '#D6D6DC'\n" +
                "    WHEN \"Role\" = 'Role 4' then '#B7B7C5'\n" +
                "    WHEN \"Role\" = 'Role 5' then '#898dadb'\n" +
                "    END as \"color\",\n" +
                "    SUM(\"Cycle Time - Days\") AS \"durationDays\",\n" +
                "    'Role' AS \"type\",\n" +
                "    CASE WHEN \"Role\" = 'Role 2' THEN\n" +
                "        3\n" +
                "    WHEN \"Role\" = 'Role 3' THEN\n" +
                "        2\n" +
                "    WHEN \"Role\" = 'Role 4' THEN\n" +
                "        3\n" +
                "    WHEN \"Role\" = 'Role 5' THEN\n" +
                "        1\n" +
                "    END AS \"standardMin\",\n" +
                "    CASE WHEN \"Role\" = 'Role 2' THEN\n" +
                "        9\n" +
                "    WHEN \"Role\" = 'Role 3' THEN\n" +
                "        4\n" +
                "    WHEN \"Role\" = 'Role 4' THEN\n" +
                "        6\n" +
                "    WHEN \"Role\" = 'Role 5' THEN\n" +
                "        3\n" +
                "    END AS \"standardMax\"\n" +
                "FROM\n" +
                "    roles a\n" +
                "    LEFT JOIN (\n" +
                "        SELECT\n" +
                "            sum(\"Cycle Time - Days\") AS \"totalTime\",\n" +
                "            \"CASE_NUMBER\"\n" +
                "        FROM\n" +
                "            roles\n" +
                "        GROUP BY\n" +
                "            CASE_NUMBER) b ON a. \"CASE_NUMBER\" = b. \"CASE_NUMBER\"\n" +
                "WHERE\n" +
                String.format("\t\"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate) +
                "    and \"totalTime\" < 1000\n" +
                "    and \"Org Code\" = 'Org 1'\n" +
                "GROUP BY\n" +
                "    a. \"CASE_NUMBER\",\n" +
                "    \"Role\",\n" +
                "    \"isActive\",\n" +
                "    \"totalTime\"\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    \"isActive\",\n" +
                "    \"totalTime\",\n" +
                "    12 AS \"totalMin\",\n" +
                "    24 AS \"totalMax\",\n" +
                "    a. \"CASE_NUMBER\" AS \"asap\",\n" +
                "    Cast(SUBSTR(a.CASE_NUMBER, 8,LENGTH(a.CASE_NUMBER)-7)as NUMBER) as num,\n" +
                "    \"Step Display Name\" AS \"name\",\n" +
                "    CASE WHEN \"Step Display Name\" = 'Step Display Name 1' then '#2a487b'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 2' then '#545E82'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 3' then '#898dadb'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 4' then '#B7B7C5'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 5' then '#D6D6DC'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 6' then '#D2D2D2'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 7' then '#f1f1f1'\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 8' then '#ffffff'\n" +
                "    END AS \"color\",\n" +
                "    SUM(\"Cycle Time - Days\") AS \"durationDays\",\n" +
                "    'Step' AS \"type\",\n" +
                "    CASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 3' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 4' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 5' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 6' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 7' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 8' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 9' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 10' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 12' THEN\n" +
                "        1\n" +
                "    END AS \"standardMin\",\n" +
                "    CASE WHEN \"Step Display Name\" = 'Step Display Name 2' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 3' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 4' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 5' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 6' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 7' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 8' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 9' THEN\n" +
                "        3\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 10' THEN\n" +
                "        1\n" +
                "    WHEN \"Step Display Name\" = 'Step Display Name 12' THEN\n" +
                "        3\n" +
                "    END AS \"standardMax\"\n" +
                "FROM\n" +
                "    steps a\n" +
                "    LEFT JOIN (\n" +
                "        SELECT\n" +
                "            sum(\"Cycle Time - Days\") AS \"totalTime\",\n" +
                "            \"CASE_NUMBER\"\n" +
                "        FROM\n" +
                "            steps\n" +
                "        GROUP BY\n" +
                "            CASE_NUMBER) b ON a. \"CASE_NUMBER\" = b. \"CASE_NUMBER\"\n" +
                "WHERE\n" +
                String.format("\t\"ASAP CREATED\" >= TO_DATE('%s', 'yyyy-MM-dd')\n", startDate) +
                String.format("\tAND \"ASAP CREATED\" < TO_DATE('%s', 'yyyy-MM-dd')\n", endDate) +
                "    and \"totalTime\" < 1000\n";

        if(orgCode.equals("organization 1")) query += "\tand \"Org Code\" = 'Org 1'\n";
        if(orgCode.equals("organization 2")) query += "\tand \"Org Code\" = 'Org 2'\n";

        query +="GROUP BY\n" +
                "    a. \"CASE_NUMBER\",\n" +
                "    \"Step Display Name\",\n" +
                "    \"isActive\",\n" +
                "    \"totalTime\")\n" +
                "    select \"isActive\", \"totalTime\", \"totalMin\", \"totalMax\", \"asap\",  \"name\", \"color\", \"durationDays\", \"type\", \"standardMin\", \"standardMax\" from combined order by \"NUM\" asc, \"name\" asc";

        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONObject result = new JSONObject();
        JSONArray resultArray = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();
            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("isActive", rs.getString("isActive"));
                item.put("totalTime", rs.getFloat("totalTime"));
                item.put("totalMin", rs.getFloat("totalMin"));
                item.put("totalMax", rs.getFloat("totalMax"));
                item.put("asap", rs.getString("asap"));
                item.put("name", rs.getString("name"));
                item.put("color", rs.getString("color"));
                item.put("durationDays", rs.getFloat("durationDays"));
                item.put("type", rs.getString("type"));
                item.put("standardMin", rs.getFloat("standardMin"));
                item.put("standardMax", rs.getFloat("standardMax"));
                resultArray.put(item);
            }
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };

        LOG.info("Counts: {}", resultArray.length());
        return resultArray;
    }

    public JSONArray cleanactivityData(JSONArray queryResult) throws JSONException {
        JSONArray cleanedArray = new JSONArray();

        JSONObject mappedObject = new JSONObject();
        for (int i = 0; i < queryResult.length(); i++) {
            JSONObject resultItem = queryResult.getJSONObject(i);

            String mainDetailsString = resultItem.getString("isActive") +
                    resultItem.getDouble("totalTime") +
                    resultItem.getDouble("totalMin") +
                    resultItem.getDouble("totalMax") +
                    resultItem.getString("asap");

            if(mappedObject.has(mainDetailsString)){
                JSONObject data = (JSONObject) mappedObject.get(mainDetailsString);
                if(resultItem.getString("type").equals("Role")) {
                    JSONArray roles = (JSONArray) data.get("roles");
                    JSONObject role = new JSONObject();
                    if(resultItem.has("name"))role.put("name", resultItem.getString("name"));
                    if(resultItem.has("type"))role.put("type", resultItem.getString("type"));
                    if(resultItem.has("color"))role.put("color", resultItem.getString("color"));
                    if(resultItem.has("durationDays"))role.put("durationDays", resultItem.getDouble("durationDays"));
                    if(resultItem.has("standardMin"))role.put("standardMin", resultItem.getDouble("standardMin"));
                    if(resultItem.has("standardMax"))role.put("standardMax", resultItem.getDouble("standardMax"));
                    roles.put(role);
                    data.put("roles", roles);
                } else {
                    JSONArray steps = (JSONArray) data.get("steps");
                    JSONObject step = new JSONObject();
                    if(resultItem.has("name"))step.put("name", resultItem.getString("name"));
                    if(resultItem.has("type"))step.put("type", resultItem.getString("type"));
                    if(resultItem.has("color"))step.put("color", resultItem.getString("color"));
                    if(resultItem.has("durationDays"))step.put("durationDays", resultItem.getDouble("durationDays"));
                    if(resultItem.has("standardMin"))step.put("standardMin", resultItem.getDouble("standardMin"));
                    if(resultItem.has("standardMax"))step.put("standardMax", resultItem.getDouble("standardMax"));
                    steps.put(step);
                    data.put("steps", steps);
                }
                mappedObject.put(mainDetailsString, data);
            } else {
                JSONObject data = new JSONObject();
                data.put("isActive", resultItem.getString("isActive"));
                data.put("totalTime", resultItem.getDouble("totalTime"));
                data.put("totalMin", resultItem.getDouble("totalMin"));
                data.put("totalMax", resultItem.getDouble("totalMax"));
                data.put("asap", resultItem.getString("asap"));
                JSONArray roles = new JSONArray();
                JSONArray steps = new JSONArray();
                JSONObject item = new JSONObject();
                if(resultItem.has("name"))item.put("name", resultItem.getString("name"));
                if(resultItem.has("type"))item.put("type", resultItem.getString("type"));
                if(resultItem.has("color"))item.put("color", resultItem.getString("color"));
                if(resultItem.has("durationDays"))item.put("durationDays", resultItem.getDouble("durationDays"));
                if(resultItem.has("standardMin"))item.put("standardMin", resultItem.getDouble("standardMin"));
                if(resultItem.has("standardMax"))item.put("standardMax", resultItem.getDouble("standardMax"));
                if(resultItem.getString("type").equals("Role"))roles.put(item);
                else steps.put(item);

                data.put("roles", roles);
                data.put("steps", steps);
                mappedObject.put(mainDetailsString, data);

            }
        }

        Iterator<String> mappedObjectKeys = mappedObject.keys();
        while (mappedObjectKeys.hasNext()) {
            String key = mappedObjectKeys.next();
            cleanedArray.put(mappedObject.get(key));
        }

        LOG.info("cleanedArray Count: {}", cleanedArray.length());
        return cleanedArray;
    }

    private JSONArray getActivityLegend(Connection connection) throws JSONException {
        JSONArray result = new JSONArray();

        JSONObject durationByStep = new JSONObject();
        String query = "--title: \"ASAP Durations by Step (Days)\"\n" +
                "select \n" +
                "\tdistinct(\"Step Display Name\") ,\n" +
                "\tCASE WHEN \"Step Display Name\" = 'Step Display Name 1' then '#2a487b'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 2' then '#545E82'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 3' then '#898dadb'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 4' then '#B7B7C5'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 5' then '#D6D6DC'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 6' then '#D2D2D2'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 7' then '#f1f1f1'\n" +
                "\tWHEN \"Step Display Name\" = 'Step Display Name 8' then '#ffffff'\n" +
                "\tEND AS \"color\"\n" +
                "from \"ADMIN\".\"sample_data_2\"";

        JSONArray ASAPDurationByStep = getDurationsStep(connection, query);
        durationByStep.put("title", "ASAP Durations by Step (Days)");
        durationByStep.put("rows", ASAPDurationByStep);
        result.put(durationByStep);

        query = " {\n" +
                "   \"title\": \"Total Time\",\n" +
                "   \"rows\": [\n" +
                "       { color: \"#719546\", name: \"Above Standards\" },\n" +
                "       { color: \"#FCD360\", name: \"Within Standards\" },\n" +
                "       { color: \"#A3294D\", name: \"Below Standards\" },\n" +
                "       { color: \"#ffffff\", name: \"Active\" },\n" +
                "   ]\n" +
                "}";
        result.put(new JSONObject(query));

        JSONObject durationByRole = new JSONObject();
        query = "--title: \"ASAP Durations by Role (Days)\"\n" +
                "select \n" +
                "\tdistinct(\"Role\") ,\n" +
                "\tCASE WHEN \"Role\" = 'Role 1' then '#B7B7C5'\n" +
                "\tWHEN \"Role\" = 'Role 2' then '#D2D2D2'\n" +
                "\tWHEN \"Role\" = 'Role 3' then '#D6D6DC'\n" +
                "\tWHEN \"Role\" = 'Role 4' then '#B7B7C5'\n" +
                "\tWHEN \"Role\" = 'Role 5' then '#898dadb'\n" +
                "\tEND as \"color\"\n" +
                "from \"ADMIN\".\"sample_data_2\" ";
        JSONArray ASAPDurationByRole = getDurationsRole(connection, query);
        durationByRole.put("title", "ASAP Durations by Role (Days)");
        durationByRole.put("rows", ASAPDurationByRole);
        result.put(durationByRole);

        return result;
    }

    public JSONArray getDurationsStep(Connection connection, String query) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray result = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();

            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("name", rs.getString("Step Display Name"));
                item.put("color", rs.getString("color"));
                result.put(item);
            }
            LOG.info("Duration Step Counts: {}", result.length());
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };
        return result;
    }

    public JSONArray getDurationsRole(Connection connection, String query) {
        PreparedStatement prepStmt = null;
        ResultSet rs = null;
        JSONArray result = new JSONArray();

        try {
            prepStmt = connection.prepareStatement(query);
            rs = prepStmt.executeQuery();

            while (rs.next()){
                JSONObject item = new JSONObject();
                item.put("name", rs.getString("Role"));
                item.put("color", rs.getString("color"));
                result.put(item);
            }
            LOG.info("Duration Role Counts: {}", result.length());
        } catch (SQLException | JSONException e) {
            e.printStackTrace();
        };
        return result;
    }
}
