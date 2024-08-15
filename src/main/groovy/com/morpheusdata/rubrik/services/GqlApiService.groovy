package com.morpheusdata.rubrik.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.AccountCredential
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.queries.GqlQueryConstants
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.client.utils.URIBuilder

import java.nio.file.Path

@Slf4j
class GqlApiService implements PlatformApiServiceInterface {

	private MorpheusContext morpheusContext
	GqlApiService(MorpheusContext morpheusContext) {
		this.morpheusContext = morpheusContext
	}

	static tokenBuffer = 43200l * 10l
	@Override
	Map getAuthConfig(BackupProvider backupProviderModel) {
		if(!backupProviderModel.credentialLoaded) {
			AccountCredential accountCredential = null
			try {
				backupProviderModel = morpheusContext.services.backupProvider.get(backupProviderModel.id)
				accountCredential = morpheusContext.services.accountCredential.loadCredentials(backupProviderModel)
			} catch (e) {
				log.error("could not load credentials: ${e}")
			}
			backupProviderModel.credentialLoaded = true
			backupProviderModel.credentialData = accountCredential?.data
		}

		def rtn = [
				apiUrl: backupProviderModel.serviceUrl,
				apiVersion: 'v1',
				token: backupProviderModel.serviceToken,
				expires: '',
				gqlPath: '/api/graphql',
				basePath: '/api'
		]

		// username is client_id, password is client_secret, service token is access token
		if(!backupProviderModel.serviceToken) {
			def localCredentials = (backupProviderModel.credentialData?.type == 'local' ? 'local' : 'client-id-secret') == 'local'
			log.info("LOCAL: ${localCredentials}")
			if(localCredentials && backupProviderModel.getConfigProperty("username") && backupProviderModel.getConfigProperty("password")) {
				log.info("LOCAL CREDENTIALS")
				rtn.username = backupProviderModel.getConfigProperty("username")
				rtn.password = backupProviderModel.getConfigProperty("password")
			}

			if(!localCredentials && backupProviderModel.credentialData.username && backupProviderModel.credentialData.password) {
				log.info("STORED CREDENTIALS")
				rtn.username = backupProviderModel.credentialData.username
				rtn.password = backupProviderModel.credentialData.password
			}
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
				String apiUrl = authConfig.apiUrl.toString()
				String apiPath = authConfig.basePath + '/client_token'
				String username = authConfig.username.toString()
				String password = authConfig.password.toString()
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(ignoreSSL: true)
				requestOpts.body = [ "client_id":username, "client_secret":password ]
				String method = "POST"
				log.info("getToken authConfig: ${authConfig}")
				log.info("apiUrl: ${apiUrl}, apiPath: ${apiPath}, user: ${username}, pass: ${password}, requestOpts: ${requestOpts}, method: ${method}")

				HttpApiClient client = new HttpApiClient()
				ServiceResponse results = client.callJsonApi(apiUrl, apiPath, username, password, requestOpts, method)
				log.info("results: ${results}")
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
		log.info("authConfig:${authConfig}, path:${path}")
		def rtn = ServiceResponse.prepare()
		try {
			def tokenResults = getToken(authConfig)
			log.info("API Token results : ${tokenResults}")
			if(tokenResults.success == true) {
				authConfig.token = tokenResults.data.access_token
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
					HttpApiClient client = new HttpApiClient()
					log.info("265 API URL: ${apiUrl}, API PATH: ${apiPath}, REQUESTOPTS: ${requestOpts}, REQUESTMETHOD: ${requestMethod}")
					log.info("266 PATH: ${path}, BODY: ${requestOpts.body}, QUERYPARAMS: ${requestOpts.queryParams}, HEADERS: ${requestOpts.headers}")
					results = client.callJsonApi(apiUrl, apiPath, requestOpts, requestMethod)
					log.info("API Result: ${results}")
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
