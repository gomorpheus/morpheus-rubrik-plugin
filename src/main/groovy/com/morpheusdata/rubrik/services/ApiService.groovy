package com.morpheusdata.rubrik.services

import com.morpheusdata.core.util.RestApiUtil
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.response.WorkloadResponse
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.tools.ant.types.spi.Service

@Slf4j
class ApiService {

	Map getAuthConfig(BackupProvider backupProviderModel) {
		def rtn = [
			apiUrl: backupProviderModel.serviceUrl,
			apiVersion: 'v1',
			token: backupProviderModel.credentialData?.password ?: backupProviderModel.serviceToken,
			basePath: '/api/v1'
		]
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
			def apiPath = authConfig.basePath + '/session'
			RestApiUtil.RestOptions requestOpts = new RestApiUtil.RestOptions(ignoreSSL: true)
			ServiceResponse results = RestApiUtil.callJsonApi(authConfig.apiUrl, apiPath, authConfig.username, authConfig.password, requestOpts, 'POST')
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
		if(authConfig.sessionId) {
			def apiPath = authConfig.basePath + '/session/' + authConfig.sessionId
			def headers = buildHeaders([:], authConfig.token)
			RestApiUtil.RestOptions requestOpts = new RestApiUtil.RestOptions(headers:headers, ignoreSSL: true)
			def results = RestApiUtil.callJsonApi(authConfig.apiUrl, apiPath, requestOpts, 'DELETE')
			rtn.success = results?.success && results?.error != true
		}
		return rtn
	}

	ServiceResponse listHosts(Map authConfig) {
		return internalGetApiRequest(authConfig, '/host', 'hosts')
	}

	ServiceResponse listSlaDomains(Map authConfig) {
		return internalGetApiRequest(authConfig, '/sla_domain', 'slaDomains')
	}

	//---- utility methods

	private ServiceResponse internalGetApiRequest(Map authConfig, String path, String dataKey='data', Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, path, 'GET', dataKey, null, queryParams, headers)
	}

	private ServiceResponse internalPostApiRequest(Map authConfig, String path, String dataKey='data', Map body=null, Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, path, 'POST', dataKey, body, queryParams, headers)
	}

	private ServiceResponse internalPatchApiRequest(Map authConfig, String path, String dataKey='data', Map body=null, Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, path, 'PATCH', dataKey, body, queryParams, headers)
	}

	private ServiceResponse internalDeleteApiRequest(Map authConfig, String path, Map queryParams=null, Map headers=null) {
		internalApiRequest(authConfig, path, 'DELETE', null, null, queryParams, headers)
	}

	private ServiceResponse internalApiRequest(Map authConfig, String path, String requestMethod='GET', String dataKey='data', Map body=null, Map queryParams=null, Map addHeaders=null) {
		def rtn = ServiceResponse.prepare()
		try {
			def tokenResults = getToken(authConfig)
			log.debug("API Token results : ${tokenResults}")
			if(tokenResults.success == true) {
				log.debug("basePath: ${authConfig.basePath}, path: ${path}")
				String tmpPath = (authConfig.basePath?.endsWith("/") ? authConfig.basePath : authConfig.basePath + "/") + (path.startsWith("/") ? path.substring(1) : path)
				log.debug("tmpPath: ${tmpPath}")
				def (String apiUrl, String apiPath) = buildApiParts(authConfig.apiUrl, tmpPath)
				log.debug("apiUrl: ${apiUrl}, apiPath: ${apiPath}")
				Map<String,String> headers = buildHeaders(addHeaders, authConfig.token)
				RestApiUtil.RestOptions requestOpts = new RestApiUtil.RestOptions(headers:headers)
				if(queryParams) {
					requestOpts.queryParams = queryParams
				}
				if(body) {
					requestOpts.body = body
				}

				ServiceResponse results = ServiceResponse.success([hasMore: true])
				rtn.data = [(dataKey):[], total:0]
				while(results.success && results.data?.hasMore) {
					results = RestApiUtil.callJsonApi(apiUrl, apiPath, requestOpts, requestMethod)
					log.debug("API Result: ${results}")
					if(results.success == true && results.hasErrors() == false) {
						if(results.data.data) {
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
