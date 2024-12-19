package com.morpheusdata.rubrik.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
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

	static tokenBuffer = 1000l * 10l
	@Override
	Map getAuthConfig(BackupProvider backupProviderModel) {
		log.debug("BACKUPPROVIDERMODEL: ${backupProviderModel.uuid}")
		try {
			if(!backupProviderModel.credentialLoaded && backupProviderModel.id) {
				AccountCredential accountCredential = morpheusContext.services.accountCredential.loadCredentials(backupProviderModel)
				backupProviderModel.credentialLoaded = true
				backupProviderModel.credentialData = accountCredential?.data
				log.debug("ACCOUNT CREDS AUTHCONFIG: ${accountCredential}")
			}

		} catch (e) {
			log.error("could not load credentials: ${e}")
		}

		def rtn = [
				apiUrl: backupProviderModel.serviceUrl,
				apiVersion: 'v1',
				token: '',
				expires: '',
				gqlPath: '/api/graphql',
				basePath: '/api'
		]

		// username is client_id, password is client_secret, service token is access token

		rtn.username = backupProviderModel.credentialData?.username ?: backupProviderModel.getConfigProperty("username")
		rtn.password = backupProviderModel.credentialData?.password ?: backupProviderModel.getConfigProperty("password")

		return rtn
	}

	@Override
	ServiceResponse getToken(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		rtn.data = [:]
		def requestToken = true
		log.debug("IN GET TOKEN 74")
		log.debug("AUTHCONFIG: ${authConfig}")
		if(authConfig.token) {
			log.debug("HAS TOKEN: ${authConfig.token}")
			log.debug("EXPIRES: ${authConfig.expires.time}")
			if (authConfig.expires) {
				// check if token in authConfig is valid

				if (authConfig.expires && authConfig.expires.time < (System.currentTimeMillis() - tokenBuffer)) {
					log.debug("api access token is expired, expires: ${authConfig.expires}, re-authenticating now to get a new token")
					requestToken = true
				} else {
					requestToken = false
					rtn.success = true
					rtn.data.token = authConfig.token
				}
			} else {
				log.debug("api access token is valid, expires: ${authConfig.expires}, using existing token")
				requestToken = false
				rtn.success = true
				rtn.data.token = authConfig.token
			}
		}

		// if the token in authConfig is invalid, retrieve cached valid token, or request new token
		if(requestToken == true) {
			log.debug("REQUESTING TOKEN 99")
			def cachedToken = getCachedToken(authConfig.username)
			log.debug("CACHED TOKEN 101: ${cachedToken}")
			def updateTokenCache = false
			if(cachedToken?.token) {
				rtn.success = true
				rtn.data.token = cachedToken.token
				rtn.data.expires = cachedToken.expires
				log.debug("RETRIEVED CACHED TOKEN: ${rtn.data}")
			} else {
				log.debug("API CALL TO REQUEST TOKEN 108")
				String apiUrl = authConfig.apiUrl.toString()
				String apiPath = authConfig.basePath + '/client_token'
				String username = authConfig.username.toString()
				String password = authConfig.password.toString()
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(ignoreSSL: true)
				requestOpts.body = [ "client_id":username, "client_secret":password ]
				String method = "POST"
				log.debug("getToken authConfig: ${authConfig}")
				log.debug("apiUrl: ${apiUrl}, apiPath: ${apiPath}, user: ${username}, pass: ${password}, requestOpts: ${requestOpts}, method: ${method}")

				HttpApiClient client = new HttpApiClient()
				ServiceResponse results = client.callJsonApi(apiUrl, apiPath, username, password, requestOpts, method)
				log.debug("results: ${results}")
				rtn.success = results?.success && results?.error != true
				if(rtn.success == true) {
					log.debug("RETRIEVED FRESH API TOKEN")
					rtn = results
					rtn.data.token = results?.data.access_token
					rtn.data.expires = new Date(System.currentTimeMillis() + (results?.data.expires_in.toLong() * 1000l))
					updateTokenCache = true
				}
			}

			// update authConfig token
			if(rtn.success) {
				log.debug("Successfully retrieved an api token that expires: ${rtn.data.expires}")
				authConfig.token = rtn.data.token
				authConfig.expires = rtn.data.expires
				if(updateTokenCache) {
					log.debug("CACHE NEW TOKEN")
					cacheToken(authConfig.username, authConfig)
				}
			}
		}
		return rtn
	}

	static Thread tokenReaperThread
	static tokens = [:] // key: client id (username)
	static tokenLock = new Object()

	// reap expired tokens and return a valid token given the client id/username
	static getCachedToken(String cacheKey) {
		log.debug("GET CACHED TOKEN")
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
				log.debug("GET CACHED TOKEN EXPIRES: ${cachedToken?.expires}")
				if(cachedToken) {
					if(cachedToken.expires > new Date(System.currentTimeMillis() + (10l*60l*1000l))) {
						rtn = cachedToken
					}

				}
			}
		} catch(Exception ex) {
			log.error("getCachedToken error: ${}", ex)
		}
		log.debug("GET CACHED TOKEN RTN: ${rtn}")
		return rtn
	}

	static void cacheToken(String cacheKey, Map authConfig) {
		log.debug("CACHING TOKEN 178")
		log.debug("CACHE KEY: ${cacheKey}")
		log.debug("CACHE DATA: ${authConfig}")
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
					log.debug("CHECKING TOKEN EXPIRES FOR REAP: ${tokenExpires}")
					if(tokenExpires && tokenExpires < new Date(System.currentTimeMillis() - (10l*60l*1000l))) {
						log.debug("REAPING TOKEN THAT EXPIRES AT: ${tokenExpires}")
						expiredKeys << cacheKey
					}
				}
				expiredKeys.each { cacheKey ->
					log.debug("REAPING TOKEN: ${cacheKey}")
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
		return internalPostApiRequest(authConfig, 'nodes', 'physicalHosts', payload, null, headers)

	}

	@Override
	ServiceResponse listSlaDomains(Map authConfig) {
		String query = GqlQueryConstants.listSlaDomains.replaceAll("[\\r\\n]", "")
		def payload = [
				"query": query,
				"operationName": "listSlaDomains"
		]
		def headers = ["Content-Type": "application/json"]
		return internalPostApiRequest(authConfig, 'nodes', 'slaDomains', payload, null, headers)
	}

	//---- utility methods
	@Override
	ServiceResponse internalApiRequest(Map authConfig, String path, String requestMethod='POST', String dataKey='data', Map body=null, Map queryParams=null, Map addHeaders=null) {
		def rtn = ServiceResponse.prepare()
		try {
			def tokenResults = getToken(authConfig)
			if(tokenResults.success == true) {
				authConfig.token = tokenResults.data.token
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
					log.debug("274 API URL: ${apiUrl}, API PATH: ${apiPath}, REQUESTOPTS: ${requestOpts}, REQUESTMETHOD: ${requestMethod}")
					log.debug("275 PATH: ${path}, BODY: ${requestOpts.body}, QUERYPARAMS: ${requestOpts.queryParams}, HEADERS: ${requestOpts.headers}")
					results = client.callJsonApi(apiUrl, apiPath, requestOpts, requestMethod)
					log.debug("API Result: ${results}")
					if(results.success == true && results.hasErrors() == false && results.data?.errors == null && results.data?.data?[dataKey]?.error == null) {
						log.debug("RESULTS SUCCESS: ${results.data}")
						if(results.data.data != null) {
							if(path) {
								results.data.data[dataKey][path]?.each { row ->
									def obj = row
									log.debug("OBJ: ${obj}")
									rtn.data[dataKey] << obj
								}
							} else {
								def obj = results.data.data[dataKey]
								log.debug("OBJ: ${obj}")
								rtn.data[dataKey] << obj
							}
							rtn.data.total = results.data.total
						} else {
							rtn.data[dataKey] = results.data
						}
						rtn.success = true
					} else {
						log.debug("RESULTS FAILED: ${results}")
						rtn = results
						if(rtn.success) {
							if(rtn.data?.errors) {
								rtn.errors = rtn.data?.errors[0]
								rtn.msg = rtn.data?.errors?[0].message ?: 'error on api request'
							} else {
								rtn.errors = rtn.data?.data?[dataKey]?.error
								rtn.msg = rtn.data?.data?[dataKey]?.error?.message ?: 'error on api request'
							}
						} else {
							rtn.msg = rtn.data?.message ?: 'error on api request'
						}
						rtn.success = false
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
		log.debug("PARSED API RESULT: ${rtn}")
		return rtn
	}
}
