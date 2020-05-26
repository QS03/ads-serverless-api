package com.serverless;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HeroMetricTest {
    @Test
    /*
     *  Valid start date and end date
     * */
    public void StartnEndDateCase() {
        HeroMetricHandler handler = new HeroMetricHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-01-01");
        queryStringParameters.put("end", "2021-01-01");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void StartDateOnlyCase() {
        HeroMetricHandler handler = new HeroMetricHandler();

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
        HeroMetricHandler handler = new HeroMetricHandler();

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
        HeroMetricHandler handler = new HeroMetricHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void InvalidDateCase() {
        HeroMetricHandler handler = new HeroMetricHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-0101");
        queryStringParameters.put("end", "2021-0101");
        input.put("queryStringParameters", queryStringParameters);

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 400);
    }
}
