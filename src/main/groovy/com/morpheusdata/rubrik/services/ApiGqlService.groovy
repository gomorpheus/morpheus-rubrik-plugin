package com.morpheusdata.rubrik.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.client.utils.URIBuilder

@Slf4j
class ApiGqlService {

	Map getAuthConfig(BackupProvider backupProviderModel) {
		def rtn = [
			apiUrl: backupProviderModel.serviceUrl,
			apiVersion: 'v1',
			token: backupProviderModel.credentialData?.password ?: backupProviderModel.serviceToken,
			gqlPath: '/api/graphql',
			basePath: '/api'
		]
		// username is client_id, password is client_secret
		if(!backupProviderModel.serviceToken && backupProviderModel.username && backupProviderModel.password) {
			rtn.username = backupProviderModel.username
			rtn.password = backupProviderModel.password
		}
		log.debug("getAuthConfig: ${rtn}")
		return rtn
	}

	private ServiceResponse getToken(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		def requestToken = true
		if(authConfig.token) {
			rtn.success = true
			rtn.setData([
				token: authConfig.token,
				sessionId: authConfig.sessionId,
				organizationId: authConfig.organizationId
			])
			requestToken = false
		}
		if(requestToken == true) {
			def apiPath = authConfig.basePath + '/client_token'
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(ignoreSSL: true)
			ServiceResponse results = HttpApiClient.callJsonApi(authConfig.apiUrl, apiPath, authConfig.username, authConfig.password, requestOpts, 'POST')
			rtn = results
			rtn.success = results?.success && results?.error != true
			if(rtn.success) {
				rtn = results
				authConfig.token = rtn.data.token
				authConfig.sessionId = rtn.data.sessionId
				authConfig.organizationId = rtn.data.organizationId
			}
		}
		return rtn
	}

	private ServiceResponse logout(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		if(authConfig.token) {
			def apiPath = authConfig.basePath + '/session/'
			def addHeaders = ["Content-Type": "application/json"]
			Map<String,String> headers = buildHeaders(addHeaders, authConfig.token)
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, ignoreSSL: true)
			def results = HttpApiClient.callJsonApi(authConfig.apiUrl, apiPath, requestOpts, 'DELETE')
			rtn.success = results?.success && results?.error != true
		}
		return rtn
	}

	ServiceResponse listHosts(Map authConfig) {
		String query = new File('../queries/listHosts.gql').text.replaceAll("[\\r\\n]", "")
		def payload = [
				"query": query,
				"operationName": "listHosts"
		]
		def headers = ["Content-Type": "application/json"]
		return internalPostApiRequest(authConfig, 'hosts', payload, null, headers)

	}

	ServiceResponse listSlaDomains(Map authConfig) {
		String query = new File('../queries/listSlaDomains.gql').text.replaceAll("[\\r\\n]", "")
		def payload = [
				"query": query,
				"operationName": "listSlaDomains"
		]
		def headers = ["Content-Type": "application/json"]
		return internalPostApiRequest(authConfig, 'slaDomains', payload, null, headers)
	}

	//---- utility methods
	private ServiceResponse internalGetApiRequest(Map authConfig, String dataKey='data', Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, 'GET', dataKey, null, queryParams, headers)
	}

	private ServiceResponse internalPostApiRequest(Map authConfig, String dataKey='data', Map body=null, Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, 'POST', dataKey, body, queryParams, headers)
	}

	private ServiceResponse internalApiRequest(Map authConfig, String requestMethod='POST', String dataKey='data', Map body=null, Map queryParams=null, Map addHeaders=null) {
		def rtn = ServiceResponse.prepare()
		try {
			def tokenResults = getToken(authConfig)
			log.debug("API Token results : ${tokenResults}")
			if(tokenResults.success == true) {
				def (String apiUrl, String apiPath) = buildApiParts(authConfig.apiUrl, authConfig.gqlPath)
				log.debug("apiUrl: ${apiUrl}, apiPath: ${apiPath}")
				Map<String,String> headers = buildHeaders(addHeaders, authConfig.token)
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
				if(queryParams) {
					requestOpts.queryParams = queryParams
				}
				if(body) {
					requestOpts.body = body
				}

				ServiceResponse results = ServiceResponse.success([hasMore: true])
				rtn.data = [(dataKey):[], total:0]
				while(results.success && results.data?.hasMore) {
					results = HttpApiClient.callJsonApi(apiUrl, apiPath, requestOpts, requestMethod)
					log.debug("API Result: ${results}")
					if(results.success == true && results.hasErrors() == false) {
						if(results.data.data != null) {
							results.data.data?.each { row ->
								def obj = row
								rtn.data[dataKey] << obj
							}
							rtn.data.total = results.data.total
						} else {
							rtn.data[dataKey] = results.data
						}
						rtn.success = true
					} else {
						rtn = results
						rtn.msg = rtn.msg ?: 'error on api request'
					}
					if(results.success && results.data?.hasMore && results.data?.links?.next?.href) {
						def parsedLink = parseApiLink(results.data.links.next.href)
						apiPath = parsedLink.path
						requestOpts.queryParams += parsedLink.query
					}
				}
			}
		} catch(e) {
			log.error("error during api request {}: {}", path, e, e)
		}
		println "\u001B[33mSL Log - internalapirequest rtn - ${rtn}\u001B[0m"

		return rtn
	}

	private Map<String,String> buildHeaders(Map<String,String> headers, String token) {
		headers = headers ?: [:]
		headers["Accept"] = "application/json"
		if(token) {
			headers["Authorization"] = "Bearer ${token}".toString()
		}
		return headers
	}

	private String extractUuid(String url) {
		def rtn = url
		def lastSlash = rtn?.lastIndexOf('/')
		if(lastSlash > -1)
			rtn = rtn.substring(lastSlash + 1)
		def queryMarker = rtn?.lastIndexOf('?')
		if(queryMarker > -1)
			rtn = rtn.substring(0, queryMarker)

		return rtn
	}

	private String extractVirtualDiskDatastore(String name) {
		def rtn
		def lastBracket = name.indexOf("]")
		if(lastBracket > -1) {
			rtn = name.substring(1, lastBracket)
		}

		return rtn
	}

	private String parseApiLink(String link) {
		if(!link.startsWith("http")) {
			link = "http://" + link
		}
		def uri = new URIBuilder(link)
		def rtn = [path: uri.path, query: [:]]
		uri.queryParams.each {
			rtn.query[it.name] = it.value
		}

		return rtn
	}

	private ArrayList<String> buildApiParts(String apiUrl, String apiPath) {
		ArrayList<String> rtn = []
		URIBuilder apiUriBuilder = new URIBuilder(apiUrl)
		rtn << apiUriBuilder.toString()
		apiUriBuilder.setPath(apiPath)
		rtn << apiUriBuilder.getPath()

		return rtn
	}
}
