package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext

class RubrikVmwareApiService {
    private RubrikVmwareRestApiService rubrikVmwareRestApiService
    private RubrikVmwareGqlApiService rubrikVmwareGqlApiService
    private MorpheusContext morpheusContext

    RubrikVmwareApiService(MorpheusContext morpheusContext) {
        this.rubrikVmwareRestApiService = new RubrikVmwareRestApiService()
        this.rubrikVmwareGqlApiService = new RubrikVmwareGqlApiService(morpheusContext)
        this.morpheusContext = morpheusContext
    }

    RubrikVmwarePlatformApiServiceInterface getPlatformApiService(String platformType) {
        if(platformType == "RSC") {
            return rubrikVmwareGqlApiService
        } else {
            return rubrikVmwareRestApiService
        }
    }
}
