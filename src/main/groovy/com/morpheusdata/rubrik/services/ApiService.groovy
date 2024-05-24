package com.morpheusdata.rubrik.services

class ApiService {
    private RestApiService restApiService
    private GqlApiService gqlApiService

    ApiService() {
        this.restApiService = new RestApiService()
        this.gqlApiService = new GqlApiService()
    }

    PlatformApiServiceInterface getPlatformApiService(String platformType) {
        if(platformType == "RSC") {
            return gqlApiService
        } else {
            return restApiService
        }
    }
}