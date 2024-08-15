package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.GqlApiService
import com.morpheusdata.rubrik.vmware.queries.RubrikVmwareGqlQueryConstants
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareGqlApiService extends GqlApiService implements RubrikVmwarePlatformApiServiceInterface {
    RubrikVmwareGqlApiService(MorpheusContext morpheusContext) {
        super(morpheusContext)
    }

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

    ServiceResponse getVirtualMachineByMoid(Map authConfig, String vmExternalId, String hostExternalId) {
        ServiceResponse rtn = ServiceResponse.prepare()
        String query = RubrikVmwareGqlQueryConstants.getVirtualMachineId.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachineId",
                "variables": [
                        "moid": vmExternalId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        def response = internalPostApiRequest(authConfig, null, 'virtualMachine', payload, null, headers)
        rtn.data = findMatchedCluster(authConfig, response, hostExternalId)
        rtn.success = true
        return rtn
    }

    Map findMatchedCluster(Map authConfig, ServiceResponse vmIdResponse, String parentServerExternalId) {
        vmIdResponse.data.vSphereVmNewConnection.nodes.find { node ->
            def vmHost = node.physicalPath.find { it ->
                it.objectType == "VSphereHost"
            }

            def host = getHostById(authConfig, vmHost.fid)
            String hostId = host.data.vSphereHost.cdmId.split("-host-")[1]

            if (parentServerExternalId == hostId) {
                return true
            } else {
                log.error("vm host id does not match parent id")
                return false
            }
        }
    }

    ServiceResponse getHostById(Map authConfig, String hostId) {
        String query = RubrikVmwareGqlQueryConstants.getHostId.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getHostId",
                "variables": [
                        "fid": hostId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'hostId', payload,null, headers)
    }

    ServiceResponse updateVirtualMachine(Map authConfig, String vmId, Map vmOpts, Map opts = [:]) {
        ServiceResponse rtn = ServiceResponse.prepare()
        try {
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

        } catch(e) {
            log.error("error updating virtual machine: {}", e, e)
        }
        return rtn
    }

    ServiceResponse backupVirtualMachine(Map authConfig, String vmId, Map opts = [:]) {
        String query = RubrikVmwareGqlQueryConstants.backupVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "backupVirtualMachine",
                "variables": [
                        "id": vmId,
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

    ServiceResponse listSnapshotsForVirtualMachine(Map authConfig, vmId) {
        def rtn = ServiceResponse.prepare()
        try {
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

        } catch(e) {
            log.error("error listing snapshots: ${e}", e)
        }
        return rtn
    }

    // was `getRequest`
    ServiceResponse getVmTaskRequest(Map authConfig, String clusterId, String requestId) {
        String query = RubrikVmwareGqlQueryConstants.getVmTaskRequest.replaceAll("[\\r\\n]", "")
        def payload = [
                "query"        : query,
                "operationName": "getVmTaskRequest",
                "variables"    : ["clusterUuid": clusterId, "requestId": requestId]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'request', payload, null, headers)

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
                "variables": [
                        "id": snapshotId,
                        "location": "V1_DELETE_VMWARE_SNAPSHOT_REQUEST_LOCATION_ALL"
                ]
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

    ServiceResponse waitForVirtualMachine(Map authConfig, String vmExternalId, String hostExternalId, backupProvider) {
        log.debug("Waiting for virtual machine {} to populate in Rubrik", vmExternalId)
        ServiceResponse rtn = ServiceResponse.prepare()
        ServiceResponse vmIdResponse = ServiceResponse.prepare()
        def attempt = 0
        def maxAttempts = 20
        def keepGoing = true
        // wait for the vm details to show up in the rubrik api. This is most critical after the initial provision or after a clone.
        while((!vmIdResponse.success || !vmIdResponse.data?.virtualMachine?.getAt(0)?.id) && keepGoing) {
            vmIdResponse = getVirtualMachineByMoid(authConfig, vmExternalId, hostExternalId)
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

    ServiceResponse waitForRestoredVirtualMachine(Map authConfig, String clusterId, String restoreRequestId) {
        log.debug("Waiting for restored virtual machine {} to populate in Rubrik", restoreRequestId)
        ServiceResponse rtn = ServiceResponse.prepare()
        def restoreRequestResult = ServiceResponse.prepare()
        def attempt = 0
        def maxAttempts = 20
        def keepGoing = true

        while(keepGoing) {
            restoreRequestResult = getVmTaskRequest(authConfig, clusterId, restoreRequestId)
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
