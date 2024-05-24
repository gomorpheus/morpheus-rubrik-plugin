package com.morpheusdata.rubrik.vmware.services


import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.GqlApiService
import com.morpheusdata.rubrik.vmware.queries.RubrikVmwareGqlQueryConstants
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareGqlApiService extends GqlApiService implements RubrikVmwarePlatformApiServiceInterface {

    ServiceResponse listVirtualMachines(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listVirtualMachines.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVirtualMachines"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'virtualMachines', payload,null, headers)
    }

    ServiceResponse getVirtualMachine(Map authConfig, String vmId) {
        String query = RubrikVmwareGqlQueryConstants.getVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachine",
                "variables" : [
                        "id": vmId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'virtualMachine', payload, null, headers)
    }

    ServiceResponse getVirtualMachineId(Map authConfig, String vmExternalId) {
        String query = RubrikVmwareGqlQueryConstants.listVirtualMachines.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachineId"
        ]
        def headers = ["Content-Type": "application/json"]
        def response = internalPostApiRequest(authConfig, null, 'virtualMachine', payload,null, headers)

        def vms = response.data['virtualMachine'].data.vSphereVmNewConnection.nodes
        def match = vms.findAll { "vm-" + it.cdmId.split("-vm-")[1] == vmExternalId}

        response.data['virtualMachine'].data.vSphereVmNewConnection.nodes = match

        return response
    }


    ServiceResponse updateVirtualMachine(Map authConfig, String vmExternalId, Map vmOpts, Map opts = [:]) {
        ServiceResponse rtn = ServiceResponse.prepare()
        try {
            def vmIdResults = getVirtualMachineId(authConfig, vmExternalId)
            log.debug("vmIdResults: ${vmIdResults}")
            def vmData = vmIdResults.data.virtualMachine instanceof List ?  vmIdResults.data.virtualMachine.getAt(0) : vmIdResults.data.virtualMachine
            if(vmIdResults.success && vmData?.id) {
                String vmId = vmData.id
                log.debug("vmId: ${vmId}")
                log.debug("vmOpts: ${vmOpts}")

                String query = RubrikVmwareGqlQueryConstants.updateVirtualMachine.replaceAll("[\\r\\n]", "")
                def payload = [
                        "query": query,
                        "operationName": "updateVirtualMachine",
                        "variables": [
                                "id": vmExternalId,
                                "configuredSlaDomainId": vmOpts.configuredSlaDomainId
                        ]
                ]

                def headers = ["Content-Type": "application/json"]
                rtn = internalPostApiRequest(authConfig, null, 'virtualMachine', payload,null, headers)
            } else {
                rtn.msg = "VM not found in Rubrik."
            }
        } catch(e) {
            log.error("error updating virtual machine: {}", e, e)
        }
        return rtn
    }

    ServiceResponse backupVirtualMachine(Map authConfig, String vmExternalId, Map opts = [:]) {
        String query = RubrikVmwareGqlQueryConstants.backupVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "backupVirtualMachine",
                "variables": [
                        "id": vmExternalId,
                        "slaId": opts.slaId
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'backupRequest', payload,null, headers)
    }

    ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = RubrikVmwareGqlQueryConstants.restoreSnapshotToVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "restoreSnapshotToVirtualMachine",
                "variables": [
                        "snapshotId": snapshotId,
                        "id": vmId,
                        "disableNetwork": false,
                        "removeNetworkDevices": false,
                        "keepMacAddresses": true,
                        "preserveMoid": true,
                        "powerOn": true
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'restoreRequest', payload,null, headers)
    }

    ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = RubrikVmwareGqlQueryConstants.restoreSnapshotToNewVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "restoreSnapshotToNewVirtualMachine",
                "variables": [
                        "id": vmId,
                        "snapshotId": snapshotId,
                        "hostId": opts.hostId,
                        "vmName": opts.vmName,
                        "storageLocationId": opts.datastoreId,
                        "disableNetwork": false,
                        "powerOn": true
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'restoreRequest', payload,null, headers)
    }

    ServiceResponse getMount(Map authConfig, String mountId) {
        String query = RubrikVmwareGqlQueryConstants.getMount.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getMount",
                "variables": [
                        "fid": mountId
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'mount', payload,null, headers)
    }

    @Override
    ServiceResponse listHosts(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listHosts.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listHosts"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'hosts', payload, null, headers)
    }

    ServiceResponse getHost(Map authConfig, String hostId) {
        String query = RubrikVmwareGqlQueryConstants.getHost.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getHost",
                "variables": [
                        "fid": hostId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'host', payload,null, headers)
    }

    ServiceResponse getVirtualDisk(Map authConfig, String diskId) {
        // no equivalent function for gql
        return internalGetApiRequest(authConfig, '/vmware/vm/virtual_disk/' + diskId, 'virtualDisk')
    }

    ServiceResponse listSnapshotsForVirtualMachine(Map authConfig, vmExternalId) {
        def rtn = ServiceResponse.prepare()
        try {
            ServiceResponse vmIdResults = getVirtualMachineId(authConfig, vmExternalId)
            log.debug("vmIdResults: ${vmIdResults}")
            def vmData = vmIdResults.data.virtualMachine instanceof List ?  vmIdResults.data.virtualMachine.getAt(0) : vmIdResults.data.virtualMachine

            if(vmIdResults.success && vmData?.id) {
                String vmId = vmData.id

                String query = RubrikVmwareGqlQueryConstants.listSnapshotsForVirtualMachine.replaceAll("[\\r\\n]", "")
                def payload = [
                        "query": query,
                        "operationName": "listSnapshotsForVirtualMachine",
                        "variables": [
                                "workloadId": vmId
                        ]
                ]
                def headers = ["Content-Type": "application/json"]
                rtn = internalPostApiRequest(authConfig, null, 'snapshots', payload,null, headers)

            } else {
                rtn.msg = "VM not found in Rubrik."
            }
        } catch(e) {
            log.error("error listing snapshots: ${e}", e)
        }
        return rtn
    }

    // was `getRequest`
    ServiceResponse getVmTaskRequest(Map authConfig, String requestId) {
        String query = RubrikVmwareGqlQueryConstants.getVmTaskRequest.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVmTaskRequest",
                "variables": [ "clusterUuid": "", "requestId": requestId ] // need to get cluster uuid
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'request', payload,null, headers)
    }

    ServiceResponse getSnapshot(authConfig, snapshotId) {
        String query = RubrikVmwareGqlQueryConstants.getSnapshot.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getSnapshot",
                "variables": [
                        "snapshotId": snapshotId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'snapshot', payload,null, headers)
    }

    ServiceResponse deleteSnapshot(authConfig, snapshotId) {
        String query = RubrikVmwareGqlQueryConstants.deleteSnapshot.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "deleteSnapshot",
                "variables": [ "snapshotIds": [ snapshotId ]]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'snapshot', payload,null, headers)
    }

    ServiceResponse listVCenterServers(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listVCenterServers.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVCenterServers"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'vcenterServers', payload, null, headers)
    }

    ServiceResponse refreshVcenterServer(Map authConfig, String serverId) {
        String query = RubrikVmwareGqlQueryConstants.refreshVcenterServer.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "refreshVcenterServer",
                "variables": [
                        "fid": serverId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'request', payload,null, headers)
    }

    ServiceResponse waitForVirtualMachine(Map authConfig, String vmExternalId, backupProvider) {
        log.debug("Waiting for virtual machine {} to populate in Rubrik", vmExternalId)
        ServiceResponse rtn = ServiceResponse.prepare()
        ServiceResponse vmIdResponse = ServiceResponse.prepare()
        def attempt = 0
        def maxAttempts = 20
        def keepGoing = true
        // wait for the vm details to show up in the rubrik api. This is most critical after the initial provision or after a clone.
        while((!vmIdResponse.success || !vmIdResponse.data?.virtualMachine?.getAt(0)?.id) && keepGoing) {
            vmIdResponse = getVirtualMachineId(authConfig, vmExternalId)
            log.debug("vmIdWaitResponse (for attempt ${attempt}): ${vmIdResponse}")
            if(vmIdResponse.success && vmIdResponse.data.virtualMachine?.getAt(0)?.id) {
                log.debug("Virtual Machine now available in Rubrik")
                rtn.success = true
                rtn.data = [virtualMachine: [id: vmIdResponse.data.virtualMachine?.getAt(0)?.id]]
                keepGoing = false
            } else {
                if(attempt == 0) {
                    log.debug("virtual machine not found in Rubrik, initiating vCenter server refresh.")
                    // on first retry kick refresh vcenter servers
                    // if the vm isn't found in Rubrik the next refresh may not be for another 10 minutes,
                    // so kick off a manual refresh
                    new VcenterServerService().executeRefresh(backupProvider, authConfig)
                }
                if(attempt < maxAttempts) {
                    sleep(60 * 1000)
                    attempt++
                } else {
                    keepGoing = false
                }
            }
        }

        return rtn
    }

    ServiceResponse waitForRestoredVirtualMachine(Map authConfig, String restoreRequestId) {
        log.debug("Waiting for restored virtual machine {} to populate in Rubrik", restoreRequestId)
        ServiceResponse rtn = ServiceResponse.prepare()
        def restoreRequestResult = ServiceResponse.prepare()
        def attempt = 0
        def maxAttempts = 20
        def keepGoing = true

        while(keepGoing) {
            restoreRequestResult = getVmTaskRequest(authConfig, restoreRequestId)
            log.debug("waitForRestoredVirtualMachine (for attempt ${attempt}): ${restoreRequestResult}")
            if(restoreRequestResult.success && restoreRequestResult.data.request.id && restoreRequestResult.data.request.status == "SUCCEEDED") {
                Boolean doRetry = false
                log.debug("Restored Virtual Machine now available in Rubrik")
                def resultLink = restoreRequestResult.data.request.links.find { it.rel == "result" }
                log.debug("resultLink: ${resultLink}")
                if(resultLink) {
                    def resultId = extractUuid(resultLink.href)
                    log.debug("resultLInk ID: ${resultId}")
                    def vmDetailResults = getRestoredVirtualMachine(authConfig, resultId)
                    log.debug("vmDetailResults: ${vmDetailResults}")
                    if(vmDetailResults.success && !vmDetailResults.data.retry) {
                        rtn.data = [virtualMachine: [id: vmDetailResults.data.virtualMachine.moid]]
                        rtn.success = true
                    } else if(vmDetailResults.data.retry) {
                        doRetry = true
                    }
                }
                if(!doRetry) {
                    keepGoing = false
                }
            } else if(restoreRequestResult.data.request.status == "FAILED") {
                rtn.msg = getApiError(restoreRequestResult.data.request)
                keepGoing = false
            } else if(attempt > maxAttempts || restoreRequestResult.success == false) {
                keepGoing = false
            }

            if(keepGoing) {
                sleep(60 * 1000)
                attempt++
            }
        }

        return rtn
    }
}
