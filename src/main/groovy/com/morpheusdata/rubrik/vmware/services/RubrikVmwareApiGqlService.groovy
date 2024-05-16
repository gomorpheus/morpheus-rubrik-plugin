package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.ApiGqlService
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareApiGqlService extends ApiGqlService {

    ServiceResponse listVirtualMachines(Map authConfig) {
        String query = new File('../queries/listVirtualMachines.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVirtualMachines"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'virtualMachines', payload,null, headers)
    }

    ServiceResponse getVirtualMachine(Map authConfig, String vmId) {
        String query = new File('../queries/getVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachine",
                "variables" : [
                        "id": vmId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'virtualMachine', payload, null, headers)
    }

    ServiceResponse getVirtualMachineId(Map authConfig, String vmExternalId) {
        String query = new File('../queries/listVirtualMachines.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachineId"
        ]
        def headers = ["Content-Type": "application/json"]
        def response = internalPostApiRequest(authConfig, 'virtualMachine', payload,null, headers)

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

                String query = new File('../queries/updateVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
                def payload = [
                        "query": query,
                        "operationName": "updateVirtualMachine",
                        "variables": [
                                "id": vmExternalId,
                                "configuredSlaDomainId": vmOpts.configuredSlaDomainId
                        ]
                ]

                def headers = ["Content-Type": "application/json"]
                rtn = internalPostApiRequest(authConfig, 'virtualMachine', payload,null, headers)
            } else {
                rtn.msg = "VM not found in Rubrik."
            }
        } catch(e) {
            log.error("error updating virtual machine: {}", e, e)
        }
        return rtn
    }

    ServiceResponse backupVirtualMachine(Map authConfig, String vmExternalId, Map opts = [:]) {
        String query = new File('../queries/backupVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "backupVirtualMachine",
                "variables": [
                        "id": vmExternalId,
                        "slaId": opts.slaId
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'backupRequest', payload,null, headers)
    }

    ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = new File('../queries/restoreSnapshotToVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
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
        return internalPostApiRequest(authConfig, 'restoreRequest', payload,null, headers)
    }

    ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = new File('../queries/restoreSnapshotToNewVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
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
        return internalPostApiRequest(authConfig, 'restoreRequest', payload,null, headers)
    }

    ServiceResponse getRestoredVirtualMachine(Map authConfig, String resourceId) {
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

    ServiceResponse getMount(Map authConfig, String mountId) {
        String query = new File('../queries/getMount.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getMount",
                "variables": [
                        "fid": mountId
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'mount', payload,null, headers)
    }

    @Override
    ServiceResponse listHosts(Map authConfig) {
        String query = new File('../queries/listHosts.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listHosts"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'hosts', payload, null, headers)
    }

    ServiceResponse getHost(Map authConfig, String hostId) {
        String query = new File('../queries/getHost.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getHost",
                "variables": [
                        "fid": hostId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'host', payload,null, headers)
    }

    ServiceResponse getVirtualDisk(Map authConfig, String diskId) {
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

                String query = new File('../queries/listSnapshotsForVirtualMachine.gql').text.replaceAll("[\\r\\n]", "")
                def payload = [
                        "query": query,
                        "operationName": "listSnapshotsForVirtualMachine",
                        "variables": [
                                "workloadId": vmId
                        ]
                ]
                def headers = ["Content-Type": "application/json"]
                rtn = internalPostApiRequest(authConfig, 'snapshots', payload,null, headers)

            } else {
                rtn.msg = "VM not found in Rubrik."
            }
        } catch(e) {
            log.error("error listing snapshots: ${e}", e)
        }
        return rtn
    }

    // was `getRequest`
    ServiceResponse getVmTaskRequest(Map authConfig, String requestId, String clusterUuid) {
        String query = new File('../queries/getVmTaskRequest.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVmTaskRequest",
                "variables": [ "clusterUuid": clusterUuid, "requestId": requestId ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'request', payload,null, headers)
    }

    ServiceResponse getSnapshot(authConfig, snapshotId) {
        String query = new File('../queries/getSnapshot.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getSnapshot",
                "variables": [
                        "snapshotId": snapshotId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'snapshot', payload,null, headers)
    }

    ServiceResponse deleteSnapshot(authConfig, snapshotId) {
        String query = new File('../queries/deleteSnapshot.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "deleteSnapshot",
                "variables": [ "snapshotIds": [ snapshotId ]]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'snapshot', payload,null, headers)
    }

    ServiceResponse listVCenterServers(Map authConfig) {
        String query = new File('../queries/listVCenterServers.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVCenterServers"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'vcenterServers', payload, null, headers)
    }

    ServiceResponse refreshVcenterServer(Map authConfig, String serverId) {
        String query = new File('../queries/refreshVcenterServer.gql').text.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "refreshVcenterServer",
                "variables": [
                        "fid": serverId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'request', payload,null, headers)
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

    def getApiError(Map apiResponse) {
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
