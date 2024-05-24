package com.morpheusdata.rubrik.services
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import org.apache.http.client.utils.URIBuilder

interface PlatformApiServiceInterface {
    Map getAuthConfig(BackupProvider backupProviderModel);
    ServiceResponse getToken(Map authConfig);
    ServiceResponse logout(Map authConfig);
    ServiceResponse listHosts(Map authConfig);
    ServiceResponse listSlaDomains(Map authConfig);
    ServiceResponse internalApiRequest(Map authConfig, String path, String requestMethod, String dataKey, Map body, Map queryParams, Map addHeaders);

    default ServiceResponse internalGetApiRequest(Map authConfig, String path, String dataKey='data', Map queryParams=null, Map headers=null) {
        internalApiRequest(authConfig, path, 'GET', dataKey, null, queryParams, headers)
    }

    default ServiceResponse internalPostApiRequest(Map authConfig, String path, String dataKey='data', Map body=null, Map queryParams=null, Map headers=null) {
        internalApiRequest(authConfig, path,'POST', dataKey, body, queryParams, headers)
    }

    default Map<String,String> buildHeaders(Map<String,String> headers, String token) {
        headers = headers ?: [:]
        headers["Accept"] = "application/json"
        if(token) {
            headers["Authorization"] = "Bearer ${token}".toString()
        }
        return headers
    }

    default String extractUuid(String url) {
        def rtn = url
        def lastSlash = rtn?.lastIndexOf('/')
        if(lastSlash > -1)
            rtn = rtn.substring(lastSlash + 1)
        def queryMarker = rtn?.lastIndexOf('?')
        if(queryMarker > -1)
            rtn = rtn.substring(0, queryMarker)

        return rtn
    }

    default String extractVirtualDiskDatastore(String name) {
        def rtn
        def lastBracket = name.indexOf("]")
        if(lastBracket > -1) {
            rtn = name.substring(1, lastBracket)
        }

        return rtn
    }

    default String parseApiLink(String link) {
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

    default ArrayList<String> buildApiParts(String apiUrl, String apiPath) {
        ArrayList<String> rtn = []
        URIBuilder apiUriBuilder = new URIBuilder(apiUrl)
        rtn << apiUriBuilder.toString()
        apiUriBuilder.setPath(apiPath)
        rtn << apiUriBuilder.getPath()

        return rtn
    }
}

