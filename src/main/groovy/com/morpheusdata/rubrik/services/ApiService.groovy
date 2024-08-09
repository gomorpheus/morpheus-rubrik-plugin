package com.morpheusdata.rubrik.services

import com.morpheusdata.core.MorpheusContext

class ApiService {
    private RestApiService restApiService
    private GqlApiService gqlApiService
    private MorpheusContext morpheusContext
    ApiService() {
        this.restApiService = new RestApiService(morpheusContext)
        this.gqlApiService = new GqlApiService(morpheusContext)
    }

    PlatformApiServiceInterface getPlatformApiService(String platformType) {
        if(platformType == "RSC") {
            return gqlApiService
        } else {
            return restApiService
        }
    }
}