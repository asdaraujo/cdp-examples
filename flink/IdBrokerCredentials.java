/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.examples;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class IdBrokerCredentials {

	final static private String KNOX_TOKEN_PATH = "/dt/knoxtoken/api/v1/token";
	final static private String CAB_CREDENTIALS_PATH = "/aws-cab/cab/api/v1/credentials";
	final static private String FS_S3A_EXT_CAB_ADDRESS_PARAM = "fs.s3a.ext.cab.address";

	final static ObjectMapper objectMapper = new ObjectMapper();
	static String idBrokerBaseUrl = null;

	private static String getIdBrokerUrl() {
		Configuration conf = new Configuration();
		if (idBrokerBaseUrl == null) {
			idBrokerBaseUrl = conf.get(FS_S3A_EXT_CAB_ADDRESS_PARAM).replaceAll("/$", "");
		}
		return idBrokerBaseUrl;
	}

	private static JsonNode get(String path) throws IOException {
		return get(path, null);
	}

	private static JsonNode get(String path, String bearerToken) throws IOException {
		URL url = new URL(getIdBrokerUrl() + path);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
		if (bearerToken != null)
			con.setRequestProperty("Authorization", "Bearer " + bearerToken);
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		con.disconnect();

		return objectMapper.readTree(response.toString());
	}

	private static String getKnoxToken() throws IOException {
		JsonNode jsonNode = get(KNOX_TOKEN_PATH);
		return jsonNode.get("access_token").asText();
	}

	public static AWSStaticCredentialsProvider getCredentials() throws IOException {
		JsonNode jsonNode = get(CAB_CREDENTIALS_PATH, getKnoxToken());
		JsonNode creds = jsonNode.get("Credentials");
		String sessionToken = creds.get("SessionToken").asText();
		String accessKeyId = creds.get("AccessKeyId").asText();
		String secretAccessKey = creds.get("SecretAccessKey").asText();

		return new AWSStaticCredentialsProvider(
				new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken));
	}
}
