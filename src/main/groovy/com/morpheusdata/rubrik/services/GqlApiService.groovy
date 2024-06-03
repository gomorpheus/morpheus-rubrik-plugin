package com.morpheusdata.rubrik.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.queries.GqlQueryConstants
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.client.utils.URIBuilder

@Slf4j
class GqlApiService implements PlatformApiServiceInterface {
	static tokenBuffer = 43200l * 10l
	@Override
	Map getAuthConfig(BackupProvider backupProviderModel) {
		def rtn = [
			apiUrl: backupProviderModel.serviceUrl,
			apiVersion: 'v1',
			token: backupProviderModel.serviceToken,
			expires: '',
			gqlPath: '/api/graphql',
			basePath: '/api'
		]
		// username is client_id, password is client_secret, service token is access token
		if(!backupProviderModel.serviceToken && backupProviderModel.username && backupProviderModel.password) {
			rtn.username = backupProviderModel.username
			rtn.password = backupProviderModel.password
		}
		log.debug("getAuthConfig: ${rtn}")
		return rtn
	}

	@Override
	ServiceResponse getToken(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		def requestToken = true
		if(authConfig.token) {
			if(authConfig.expires) {
				// check if token in authConfig is valid
				def checkDate = new Date()
				def tokenValid = ((checkDate.time + tokenBuffer) <= authConfig.expires.time)
				if (!tokenValid) {
					requestToken = true
				} else {
					requestToken = false
					rtn.success = true
					rtn.token = authConfig.token
					rtn.sessionId = authConfig.sessionId
					rtn.organizationId = authConfig.organizationId
				}
			} else {
				requestToken = true
			}
		}

		// if the token in authConfig is invalid, retrieve cached valid token, or request new token
		if(requestToken == true) {
			def cachedToken = getCachedToken(authConfig.username)
			if(cachedToken?.token) {
				rtn.success = true
				rtn.token = cachedToken.token
				rtn.expires = cachedToken.expires
				rtn.sessionId = cachedToken.sessionId
				rtn.organizationId = cachedToken.organizationId
			} else {
				def apiPath = authConfig.basePath + '/client_token'
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(ignoreSSL: true)
				ServiceResponse results = HttpApiClient.callJsonApi(authConfig.apiUrl, apiPath, authConfig.username, authConfig.password, requestOpts, 'POST')
				rtn = results
				rtn.success = results?.success && results?.error != true
				cacheToken(authConfig.username, authConfig)
			}

			// update authConfig token
			if(rtn.success) {
				authConfig.token = rtn.data.token
				authConfig.sessionId = rtn.data.sessionId
				authConfig.organizationId = rtn.data.organizationId
				authConfig.expires = rtn.data.expires
			}
		}
		return rtn
	}

	static Thread tokenReaperThread
	static tokens = [:] // key: client id (username)
	static tokenLock = new Object()

	// reap expired tokens and return a valid token given the client id/username
	static getCachedToken(String cacheKey) {
		def rtn
		try {
			synchronized (tokenLock) {
				if(!tokenReaperThread) {
					tokenReaperThread = new Thread().start {
						while(true) {
							try {
								sleep(60000L)
								reapExpiredTokens()
							} catch(t2) {
								log.warn("Error Running Rubrik Access Token Reaper Thread ${}", t2)
							}
						}
					}
				}
				def cachedToken = tokens[cacheKey]
				if(cachedToken) {
					if(cachedToken.expires > new Date(new Date().time) + (10l*60l*1000l)) {
						rtn = cachedToken
					}

				}
			}
		} catch(Exception ex) {
			log.error("getCachedToken error: ${}", ex)
		}
		return rtn
	}

	static void cacheToken(String cacheKey, Map authConfig) {
		try {
			synchronized(tokenLock) {
				tokens[cacheKey] = [
					token: authConfig.token,
					expires: authConfig.expires,
					sessionId: authConfig.sessionId,
					organizationId: authConfig.organizationId
				]
			}
		} catch(Exception ex) {
			log.error("cacheToken error: ${}", ex)
		}
	}

	static reapExpiredTokens() {
		try {
			synchronized (tokenLock) {
				def expiredKeys = []
				for (cacheKey in tokens.keySet()) {
					def tokenExpires = tokens[cacheKey]?.expires
					if(tokenExpires && tokenExpires < new Date(new Date().time - (10l*60l*1000l))) {
						expiredKeys << cacheKey
					}
				}
				expiredKeys.each { cacheKey ->
					tokens.remove(cacheKey)
				}
			}
		} catch (Exception ex) {
			log.error("reapExpiredTokens error: ${}", ex)
		}
	}

	@Override
	ServiceResponse logout(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		if(authConfig.token) {
			def apiPath = authConfig.basePath + '/session'
			def addHeaders = ["Content-Type": "application/json"]
			Map<String,String> headers = buildHeaders(addHeaders, authConfig.token)
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, ignoreSSL: true)
			def results = HttpApiClient.callJsonApi(authConfig.apiUrl, apiPath, requestOpts, 'DELETE')
			rtn.success = results?.success && results?.error != true
		}
		return rtn
	}

	@Override
	ServiceResponse listHosts(Map authConfig) {
		String query = GqlQueryConstants.listHosts.replaceAll("[\\r\\n]", "")
		def payload = [
				"query": query,
				"operationName": "listHosts",
				"variables": [
				        "hostRoot": "WINDOWS_HOST_ROOT"
				]
		]
		def headers = ["Content-Type": "application/json"]
		return internalPostApiRequest(authConfig, null, 'hosts', payload, null, headers)

	}

	@Override
	ServiceResponse listSlaDomains(Map authConfig) {
		String query = GqlQueryConstants.listSlaDomains.replaceAll("[\\r\\n]", "")
		def payload = [
				"query": query,
				"operationName": "listSlaDomains"
		]
		def headers = ["Content-Type": "application/json"]
		return internalPostApiRequest(authConfig, null, 'slaDomains', payload, null, headers)
	}

	//---- utility methods
	@Override
	ServiceResponse internalApiRequest(Map authConfig, String path, String requestMethod='POST', String dataKey='data', Map body=null, Map queryParams=null, Map addHeaders=null) {
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
}
