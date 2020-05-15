package com.serverless;

import org.json.JSONObject;

public class Response {

	private final JSONObject body;
	private final Integer statusCode;

	public Response(JSONObject body, Integer statusCode) {
		this.body = body;
		this.statusCode = statusCode;
	}

	public JSONObject getBody() {
		return this.body;
	}

	public Integer getStatusCode() {
		return this.statusCode;
	}
}
