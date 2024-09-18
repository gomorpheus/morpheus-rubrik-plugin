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

	static tokenBuffer = 43200l * 10l
	@Override
	Map getAuthConfig(BackupProvider backupProviderModel) {
		log.info("BACKUPPROVIDERMODEL: ${backupProviderModel.uuid}")
		try {
			if(!backupProviderModel.credentialLoaded && backupProviderModel.id) {
				AccountCredential accountCredential = morpheusContext.services.accountCredential.loadCredentials(backupProviderModel)
				backupProviderModel.credentialLoaded = true
				backupProviderModel.credentialData = accountCredential?.data
				log.info("ACCOUNT CREDS AUTHCONFIG: ${accountCredential}")
			}

		} catch (e) {
			log.error("could not load credentials: ${e}")
		}


		//}

		def rtn = [
				apiUrl: backupProviderModel.serviceUrl,
				apiVersion: 'v1',
				token: '',
				expires: '',
				gqlPath: '/api/graphql',
				basePath: '/api'
		]

		log.info("CREDENTIAL DATA: ${backupProviderModel.credentialData}")
		// username is client_id, password is client_secret, service token is access token

		rtn.username = backupProviderModel.credentialData?.username ?: backupProviderModel.getConfigProperty("username")
		rtn.password = backupProviderModel.credentialData?.password ?: backupProviderModel.getConfigProperty("password")
//
//		if(localCredentials && backupProviderModel.getConfigProperty("username") && backupProviderModel.getConfigProperty("password")) {
//			log.info("LOCAL CREDENTIALS")
//			rtn.username = backupProviderModel.getConfigProperty("username")
//			rtn.password = backupProviderModel.getConfigProperty("password")
//		}
//
//		if(!localCredentials && backupProviderModel.credentialData.username && backupProviderModel.credentialData.password) {
//			log.info("STORED CREDENTIALS")
//			rtn.username = backupProviderModel.credentialData.username
//			rtn.password = backupProviderModel.credentialData.password
//		}


		log.info("getAuthConfig: ${rtn}")
		return rtn
	}

	@Override
	ServiceResponse getToken(Map authConfig) {
		def rtn = ServiceResponse.prepare()
		def requestToken = true
		log.info("IN GET TOKEN 74")
		if(authConfig.token) {
			log.info("HAS TOKEN: ${authConfig.token}")
			log.info("EXPIRES: ${authConfig.expires}")
			if(authConfig.expires) {
				// check if token in authConfig is valid
				def checkDate = new Date()
				log.info("AUTHCONFIG EXPIRES: ${authConfig.expires}")
				log.info("CHECKDATE: ${checkDate}")
				def tokenValid = ((checkDate.time + tokenBuffer) <= authConfig.expires.time)
				log.info("TOKEN VALID: ${authConfig.expires.time}, ${tokenValid}")
				if (!tokenValid) {
					requestToken = true
				} else {
					requestToken = false
					rtn.success = true
					rtn.data.token = authConfig.token
					rtn.data.expires = authConfig.expires
				}
			} else {
				requestToken = true
			}
		}

		// if the token in authConfig is invalid, retrieve cached valid token, or request new token
		if(requestToken == true) {
			log.info("REQUESTING TOKEN 99")
			def cachedToken = getCachedToken(authConfig.username)
			if(cachedToken?.token) {
				rtn.success = true
				rtn.data.token = cachedToken.token
				rtn.data.expires = cachedToken.expires
			} else {
				log.info("API CALL TO REQUEST TOKEN 108")
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
				if(rtn.success) {
					rtn.data.token = results?.data.access_token
					rtn.data.expires = new Date(new Date().time + results?.data.expires_in)
					log.info("TOKEN: ${rtn.data.token}")
					log.info("TOKEN EXPIRES: ${rtn.data.expires}")
				}
			}

			// update authConfig token
			if(rtn.success) {
				authConfig.token = rtn.data.token
				authConfig.expires = rtn.data.expires
				cacheToken(authConfig.username, authConfig)
			}
		}
		return rtn
	}

	static Thread tokenReaperThread
	static tokens = [:] // key: client id (username)
	static tokenLock = new Object()

	// reap expired tokens and return a valid token given the client id/username
	static getCachedToken(String cacheKey) {
		log.info("GET CACHED TOKEN")
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
				log.info("tokens: ${tokens}")
				log.info("CACHED TOKEN TYPE: ${cachedToken?.getClass()}")
				log.info("GET CACHED TOKEN EXPIRES: ${cachedToken?.expires}")
				if(cachedToken) {
					if(cachedToken.expires > new Date(new Date().time + (10l*60l*1000l))) {
						rtn = cachedToken
					}

				}
			}
		} catch(Exception ex) {
			log.error("getCachedToken error: ${}", ex)
		}
		log.info("GET CACHED TOKEN RTN: ${rtn}")
		return rtn
	}

	static void cacheToken(String cacheKey, Map authConfig) {
		log.info("CACHING TOKEN 178")
		log.info("CACHE KEY: ${cacheKey}")
		log.info("CACHE DATA: ${authConfig}")
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
					log.info("REAP EXPIRED TOKEN EXPIRES: ${tokenExpires}")
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
		log.info("authConfig:${authConfig}")
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
					log.info("274 API URL: ${apiUrl}, API PATH: ${apiPath}, REQUESTOPTS: ${requestOpts}, REQUESTMETHOD: ${requestMethod}")
					log.info("275 PATH: ${path}, BODY: ${requestOpts.body}, QUERYPARAMS: ${requestOpts.queryParams}, HEADERS: ${requestOpts.headers}")
					results = client.callJsonApi(apiUrl, apiPath, requestOpts, requestMethod)
					log.info("API Result: ${results}")
					if(results.success == true && results.hasErrors() == false && results.data?.errors == null && results.data?.data?[dataKey]?.error == null) {
						log.info("RESULTS SUCCESS: ${results.data}")
						if(results.data.data != null) {
							if(path) {
								results.data.data[dataKey][path]?.each { row ->
									def obj = row
									log.info("OBJ: ${obj}")
									rtn.data[dataKey] << obj
								}
							} else {
								def obj = results.data.data[dataKey]
								log.info("OBJ: ${obj}")
								rtn.data[dataKey] << obj
							}
							rtn.data.total = results.data.total
						} else {
							rtn.data[dataKey] = results.data
						}
						rtn.success = true
					} else {
						log.info("RESULTS FAILED: ${results}")
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
		log.info("PARSED API RESULT: ${rtn}")
		return rtn
	}
}
