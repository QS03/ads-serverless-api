package com.serverless;

import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;


public class CustomersHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private final Logger LOG = LogManager.getLogger(this.getClass());

	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		LOG.info("input: {}", input);

		DBCredentials dbCreds = new DBCredentials();
		dbCreds.setDbHost("covid-oracle.cewagdn2zv2j.us-west-2.rds.amazonaws.com");
		dbCreds.setDbPort("1521");
		dbCreds.setUserName("admin");
		dbCreds.setPassword("8iEkGjQgFJzOblCihFaz");
		dbCreds.setDbName("orcl");

		DBConnection dbConnection = new DBConnection();
		Connection connection = dbConnection.getConnection(dbCreds);		

		int statusCode = 200;
		JSONObject retObject = new JSONObject();
		JSONObject data = new JSONObject();

		String query = "SELECT count(DISTINCT (CUSTOMERNAME)) FROM ADMIN.\"sales_data_sample\"";		
		try {
			if (Optional.ofNullable(connection).isPresent()) {				
				int customerCount = runQuery(connection, query);
				data.put("count", customerCount);				
			}
			else {
				statusCode = 501;
				data.put("message", "Server error!");
			}
			retObject.put("data", data);
		} catch (JSONException e) {
			LOG.info("Error: {}", e);
		}
		
		Response responseBody = new Response(retObject, statusCode);
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
	public int runQuery(Connection connection, String query) {
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		int result = 0;

		try {
			prepStmt = connection.prepareStatement(query);
			rs = prepStmt.executeQuery();
			rs.next();
			result = rs.getInt("COUNT(DISTINCT(CUSTOMERNAME))");
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}
}
