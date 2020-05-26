package com.serverless;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

public class HeroStatsTest {

    @Test
    /*
    *  Valid start date and end date
    * */
    public void StartnEndDateCase() {
        HeroStatsHandler handler = new HeroStatsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-01-01");
        queryStringParameters.put("end", "2020-05-20");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void StartDateOnlyCase() {
        HeroStatsHandler handler = new HeroStatsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-01-01");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void EndDateOnlyCase() {
        HeroStatsHandler handler = new HeroStatsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("end", "2021-01-01");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void NoDateCase() {
        HeroStatsHandler handler = new HeroStatsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void InvalidDate() {
        HeroStatsHandler handler = new HeroStatsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020xx0101");
        queryStringParameters.put("end", "2021-01&01");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 400);
    }
}
