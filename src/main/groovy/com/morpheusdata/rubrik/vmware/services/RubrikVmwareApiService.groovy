package com.morpheusdata.rubrik.vmware.services

class RubrikVmwareApiService {
    private RubrikVmwareRestApiService rubrikVmwareRestApiService
    private RubrikVmwareGqlApiService rubrikVmwareGqlApiService

    RubrikVmwareApiService() {
        this.rubrikVmwareRestApiService = new RubrikVmwareRestApiService()
        this.rubrikVmwareGqlApiService = new RubrikVmwareGqlApiService()
    }

    RubrikVmwarePlatformApiServiceInterface getPlatformApiService(String platformType) {
        if(platformType == "RSC") {
            return rubrikVmwareGqlApiService
        } else {
            return rubrikVmwareRestApiService
        }
    }
}
