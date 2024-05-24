package com.morpheusdata.rubrik.vmware.services;

import com.morpheusdata.response.ServiceResponse;
import com.morpheusdata.rubrik.services.PlatformApiServiceInterface;

import java.util.Map;

public interface RubrikVmwarePlatformApiServiceInterface extends PlatformApiServiceInterface {
    ServiceResponse listVirtualMachines(Map authConfig);
    ServiceResponse getVirtualMachine(Map authConfig, String vmId);
    ServiceResponse getVirtualMachineId(Map authConfig, String vmExternalId);
    ServiceResponse updateVirtualMachine(Map authConfig, String vmExternalId, Map vmOpts, Map opts);
    ServiceResponse backupVirtualMachine(Map authConfig, String vmExternalId, Map opts);
    ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts);
    ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts);
    ServiceResponse getMount(Map authConfig, String mountId);
    ServiceResponse listHosts(Map authConfig);
    ServiceResponse getHost(Map authConfig, String hostId);
    ServiceResponse getVirtualDisk(Map authConfig, String diskId);
    ServiceResponse listSnapshotsForVirtualMachine(Map authConfig, vmExternalId);
    ServiceResponse getVmTaskRequest(Map authConfig, String requestId); // fix parameters
    ServiceResponse getSnapshot(authConfig, snapshotId);
    ServiceResponse deleteSnapshot(authConfig, snapshotId);
    ServiceResponse listVCenterServers(Map authConfig);
    ServiceResponse refreshVcenterServer(Map authConfig, String serverId);
    ServiceResponse waitForVirtualMachine(Map authConfig, String vmExternalId, backupProvider);
    ServiceResponse waitForRestoredVirtualMachine(Map authConfig, String restoreRequestId);


    default ServiceResponse getRestoredVirtualMachine(Map authConfig, String resourceId) {
        ServiceResponse rtn = ServiceResponse.prepare()
        try {
            def vmId
            if(resourceId.startsWith("VirtualMachine:::")) {
                vmId = resourceId
            } else {
                def mountDetailResults = getMount(authConfig, resourceId)
                if(mountDetailResults.success == false) {
                    rtn = [success: false, msg: "Mount not found", retry: true]
                }
                if(mountDetailResults.success) {
                    vmId = mountDetailResults.data.mountedVmId
                }
            }
            if(vmId) {
                def vmDetailRequest = getVirtualMachine(authConfig, vmId)
                if(vmDetailRequest.success) {
                    rtn.data = vmDetailRequest.data
                    rtn.success = true
                } else {
                    rtn.success = false
                    rtn.data = rtn.data ?: [:]
                    rtn.data.retry = true
                    rtn.msg = "Could not find restored vm reference"
                    log.error("Could not find restored vm reference")
                }
            }
        } catch (Exception e) {
            log.error("error fetching restored vm: ${e}", e)
        }

        return rtn
    }

    default def getApiError(Map apiResponse) {
        def rtn = null
        def errorMessage = [:]
        if(apiResponse?.error?.message) {
            errorMessage = new groovy.json.JsonSlurper().parseText(apiResponse.error.message)
        }
        if(errorMessage) {
            rtn = errorMessage?.cause?.reason ?: errorMessage?.message
        }
        return rtn
    }
}
