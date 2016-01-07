/**
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
#include "hdfs_http_client.h"
#include "hdfs_json_parser.h"
#include "hdfs.h"	/* For hdfs_oauth */
#include <jansson.h>
#include <string.h>


/**
 * OAuth Token structure
 */
struct oauthAccessToken{
	char* access_token;
	char* token_type;
	int expires_in;
	int expires_on;
	char* resource;
	char* refresh_token; 
};

static void freeJsonOAuth(struct oauthAccessToken* json){
	if(!json){
		if(json->access_token)
			free(json->access_token);
		if(json->token_type)
			free(json->token_type);
		if(json->resource)
			free(json->resource);
		if(json->refresh_token)
			free(json->refresh_token);

		free(json);
	}
}
 
static int parseOAuthAccessToken(const char* content, struct oauthAccessToken *json_oauth){
	json_error_t error;
    size_t flags = 0;
    const char *key = NULL;
    json_t *value;
    json_t *jobj;
	int token_set = 0;    

    if (!content) {
        return EIO;
    }
    jobj = json_loads(content, flags, &error);
    if (!jobj) {
		fprintf(stderr,"Content: %s\n",content);
        fprintf(stderr, "JSon parsing error: on line %d: %s\n",
                error.line, error.text);
        return EIO;
    }
    void *iter = json_object_iter(jobj);
    while(iter)  {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
		if(!strcmp(key,"access_token")){
			json_oauth->access_token = strdup(json_string_value(value));
			token_set = 1;
		}else if(!strcmp(key,"token_type")){
			json_oauth->token_type = strdup(json_string_value(value));
		}else if(!strcmp(key,"expires_in")){
			json_oauth->expires_in = atoi(json_string_value(value));
		}else if(!strcmp(key,"expires_on")){
			json_oauth->expires_on = atoi(json_string_value(value));;
		}
		iter = json_object_iter_next(jobj, iter);
	}
	return token_set ? 0 : EIO;
}

static int oAuthPostData(char **postData, char *refresh_token, char *client_id){
	const char *post_data_parameters = "grant_type=%s&refresh_token=%s&client_id=%s";
	const char *grant_type = "refresh_token";
	const int sizeOfPostData = (strlen(post_data_parameters)
												+ strlen(grant_type) 
												+ strlen(refresh_token)
												+ strlen(client_id) + 10 
											  );
	*postData = (char*) calloc(1 ,sizeof(char) * sizeOfPostData);

	if(!(*postData)){
		fprintf(stderr, "Cannot allocate memory for postData for oauth.");
		return EINVAL;
	}

	sprintf(*postData,post_data_parameters,grant_type, refresh_token, client_id);
	return 0;
}

int update_oauth_token(hdfs_oauth oauth){
	char *postData = NULL;
	struct webhdfsBuffer *buffer;
	struct Response *resp = NULL;
	struct oauthAccessToken* json_oauth = NULL;
	int ret = 0;

	ret = oAuthPostData(&postData, oauth->refresh_token, oauth->client_id);
	
	if(ret){
		fprintf(stderr, "failed to write postData for oauth.");
		goto done;
	}
	
	buffer = calloc(1, sizeof(struct webhdfsBuffer));
	buffer->wbuffer = postData;
    buffer->offset = 0;
	buffer->remaining = strlen(postData);
	buffer->closeFlag = 1;
	
	ret = launchUpdateOAuth(oauth->refresh_url, NULL, buffer, &resp);
	// Parse the response here...
	if(ret || !resp->body->content){
		fprintf(stderr, "Error updating oauth token.\n");
		goto done;
	}
	
	json_oauth = (struct oauthAccessToken *)calloc(0,sizeof(struct oauthAccessToken));
	ret = parseOAuthAccessToken(resp->body->content, json_oauth);

	if(ret){
		fprintf(stderr,"Parsing of oauth token failed.\n");
		goto done;
	}

	oauth->expires_at = json_oauth->expires_on;
	oauth->access_token = strdup(json_oauth->access_token);
	done:
		if(ret){
			free(postData);
			free(buffer);
			if(resp)
				freeResponse(resp);
		}
		return ret;

}


