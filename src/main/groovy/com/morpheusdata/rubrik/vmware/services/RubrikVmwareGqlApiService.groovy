package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.GqlApiService
import com.morpheusdata.rubrik.vmware.queries.RubrikVmwareGqlQueryConstants
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareGqlApiService extends GqlApiService implements RubrikVmwarePlatformApiServiceInterface {
    private MorpheusContext morpheusContext
    RubrikVmwareGqlApiService(MorpheusContext morpheusContext) {
        super(morpheusContext)
        this.morpheusContext = morpheusContext
    }

    ServiceResponse listVirtualMachines(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listVirtualMachines.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVirtualMachines"
        ]
        def headers = ["Content-Type": "application/json"]
        ServiceResponse rtn = internalPostApiRequest(authConfig, 'nodes', 'vSphereVmNewConnection', payload,null, headers)
        log.debug("LIST VIRTUAL MACHINE RTN: ${rtn}")
        if(rtn.success) {
            rtn.data.virtualMachines = rtn.data.vSphereVmNewConnection
        }
        return rtn
    }

    ServiceResponse getVirtualMachine(Map authConfig, String vmId) {
        if(vmId.startsWith("VirtualMachine:::")) {
            String cdmId = vmId.split(":::")[1]
            vmId = getVirtualMachineFidFromCdmId(authConfig, cdmId)
        }

        String query = RubrikVmwareGqlQueryConstants.getVirtualMachine.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachine",
                "variables" : [
                        "fid": vmId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vSphereDetailData', payload, null, headers)
        log.debug("GET VIRTUAL MACHINE API RESPONSE: ${rtn}")

        if (rtn.success) {
            def host = rtn.data.vSphereDetailData?[0].physicalPath.find { it ->
                it.objectType == "VSphereHost"
            }
            log.debug("GET VIRTUAL MACHINE HOST ID: ${host.fid}, ${host.fid.getClass()}")
            log.debug("GET VIRTUAL MACHINE: ${rtn.data.vSphereDetailData.getClass()}")

            rtn.data.virtualMachine = rtn.data.vSphereDetailData
            rtn.data.virtualMachine = rtn.data.virtualMachine.getAt(0) + [hostId: host.fid]
        }
        log.debug("GET VIRTUAL MACHINE RTN: ${rtn}")
        return rtn
    }

    String getVirtualMachineFidFromCdmId(Map authConfig, String vmCdmId) {
        def vms = listVirtualMachines(authConfig).data.virtualMachines
        def vm = vms.find { node ->
            node.cdmId == vmCdmId
        }

        log.debug("VM CDM ID: ${vmCdmId}")
        log.debug("VM FID: ${vm.id}")
        return vm.id
    }

    ServiceResponse getVirtualMachineByMoid(Map authConfig, String vmExternalId, String hostExternalId) {
        ServiceResponse rtn = ServiceResponse.prepare()
        String query = RubrikVmwareGqlQueryConstants.getVirtualMachineByMoid.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getVirtualMachineByMoid",
                "variables": [
                        "moid": vmExternalId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        def response = internalPostApiRequest(authConfig, 'nodes', 'vSphereVmNewConnection', payload, null, headers)
        log.debug("GETVIRTUALMACHINEBYMOID: ${vmExternalId}, ${hostExternalId}")
        log.debug("GETVIRTUALMACHINEBYMOID RESPONSE: ${response}")
        rtn.data = findMatchedCluster(authConfig, response, hostExternalId)
        rtn.success = true
        return rtn
    }

    Map findMatchedCluster(Map authConfig, ServiceResponse vmIdResponse, String parentServerExternalId) {
        log.debug("FIND MATCHED CLUSTER: ${vmIdResponse.data}, ${parentServerExternalId}")
        vmIdResponse.data.vSphereVmNewConnection.find { node ->
            def vmHost = node.physicalPath.find { it ->
                it.objectType == "VSphereHost"
            }

            def host = getHostById(authConfig, vmHost.fid)
            def cdmId = host.data.vSphereHost.cdmId
            String hostId = cdmId[0].split('host-')[1]
            log.debug("HOST ID: ${hostId}")

            if (parentServerExternalId.split('-')[1] == hostId) {
                return true
            } else {
                log.error("vm host id does not match parent id")
                return false
            }
        }
    }

    ServiceResponse getHostById(Map authConfig, String hostId) {
        String query = RubrikVmwareGqlQueryConstants.getHostById.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getHostById",
                "variables": [
                        "fid": hostId
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'vSphereHost', payload,null, headers)
    }

    ServiceResponse updateVirtualMachine(Map authConfig, String vmId, Map vmOpts, Map opts = [:]) {
        ServiceResponse rtn = ServiceResponse.prepare()
        try {
            def slaDomainAssignType
            def slaOptionalId
            if(vmOpts.configuredSlaDomainId == "UNPROTECTED") {
                slaDomainAssignType = "doNotProtect"
                slaOptionalId = null
            } else if (vmOpts.configuredSlaDomainId == "INHERIT") {
                slaDomainAssignType = "noAssignment"
                slaOptionalId = null
            } else {
                slaDomainAssignType = "protectWithSlaId"
                slaOptionalId = vmOpts.configuredSlaDomainId
            }

            String query = RubrikVmwareGqlQueryConstants.updateVirtualMachine.replaceAll("[\\r\\n]", "")
            def payload = [
                    "query": query,
                    "operationName": "updateVirtualMachine",
                    "variables": [
                            "slaDomainAssignType": slaDomainAssignType,
                            "slaOptionalId": slaOptionalId,
                            "objectIds": [ vmId ]
                    ]
            ]

            def headers = ["Content-Type": "application/json"]
            rtn = internalPostApiRequest(authConfig, null, 'updateVsphereVm', payload,null, headers)

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
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vsphereOnDemandSnapshot', payload,null, headers)
        if (rtn.success) {
            rtn.data.backupRequest = rtn.data.vsphereOnDemandSnapshot?[0]
        }
        return rtn
    }

    ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = RubrikVmwareGqlQueryConstants.restoreSnapshotToVirtualMachine.replaceAll("[\\r\\n]", "")
        String snapshotFid = getSnapshotFidFromCdmId(authConfig, vmId, snapshotId)
        log.debug("SNAPSHOT ID: ${snapshotId}")
        def payload = [
                "query": query,
                "operationName": "restoreSnapshotToVirtualMachine",
                "variables": [
                        "snapshotId": snapshotFid,
                        "id": vmId,
                        "disableNetwork": false,
                        "removeNetworkDevices": false,
                        "keepMacAddresses": true,
                        "preserveMoid": true,
                        "powerOn": true
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vsphereVmInitiateInstantRecoveryV2', payload,null, headers)
        if (rtn.success) {
            rtn.data.restoreRequest = rtn.data.vsphereVmInitiateInstantRecoveryV2?[0]
        }

        log.debug("RESTORE SNAPSHOT TO VIRTUAL MACHINE RTN: ${rtn}")
        return rtn
    }

    ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
        String query = RubrikVmwareGqlQueryConstants.restoreSnapshotToNewVirtualMachine.replaceAll("[\\r\\n]", "")
        String snapshotFid = getSnapshotFidFromCdmId(authConfig, vmId, snapshotId)
        log.debug("SNAPSHOT ID: ${snapshotId}")
        log.debug("SNAPSHOT FID: ${snapshotFid}")
        def payload = [
                "query": query,
                "operationName": "restoreSnapshotToNewVirtualMachine",
                "variables": [
                        "id": vmId,
                        "snapshotId": snapshotFid,
                        "hostId": opts.hostId,
                        "vmName": opts.vmName,
                        "storageLocationId": opts.datastoreId,
                        "disableNetwork": false,
                        "powerOn": true
                ]
        ]

        def headers = ["Content-Type": "application/json"]
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vsphereVmExportSnapshotV3', payload,null, headers)
        if (rtn.success) {
            rtn.data.restoreRequest = rtn.data.vsphereVmExportSnapshotV3?[0]
        }

        log.debug("RESTORE SNAPSHOT TO NEW VIRTUAL MACHINE RTN: ${rtn}")
        return rtn
    }

    String getSnapshotFidFromCdmId(Map authConfig, String vmId, String snapshotCdmId) {
        ServiceResponse vm = getVirtualMachine(authConfig, vmId)
        def snapshots = vm.data.virtualMachine.snapshotConnection.nodes
        log.debug("SNAPSHOTS: ${snapshots}")
        def snapshot = snapshots.find { node ->
            node.cdmId == snapshotCdmId
        }

        log.debug("VM ID: ${vmId}")
        log.debug("SNAPSHOT CDM ID: ${snapshotCdmId}")
        if(snapshot) {
            log.debug("SNAPSHOT FID: ${snapshot.id}")
            return snapshot.id
        } else {
            return snapshotCdmId
        }
    }

    ServiceResponse getMount(Map authConfig, String mountId) {
        String query = RubrikVmwareGqlQueryConstants.getMounts.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "getMounts",
        ]

        def headers = ["Content-Type": "application/json"]
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vSphereMountConnection', payload,null, headers)
        if(rtn.success) {
            def mount = rtn.data.vSphereMountConnection[0].nodes.find { node ->
                log.debug("MOUNT NODE: ${node}")
                node.cdmId == mountId
            }
            log.debug("MOUNT: ${mount}")
            rtn.data.mount = mount
            rtn.data.mount.mountedVmId = mount.newVm.id
            log.debug("MOUNTED VM ID: ${mount.newVm.id}")
        }
        return rtn
    }

    @Override
    ServiceResponse listHosts(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listHosts.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listHosts"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'nodes', 'vSphereHostConnection', payload, null, headers)
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
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vSphereHost', payload,null, headers)
        log.debug("GET HOST RESPONSE: ${rtn}")
        if(rtn.success) {
            rtn.data.host = rtn.data.vSphereHost?[0]
            rtn.data.host.datastores = rtn.data.vSphereHost[0].descendantConnection.nodes
            log.debug("DATASTORES: ${rtn.data.host.datastores}")
        }
        return rtn
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
            rtn = internalPostApiRequest(authConfig, 'nodes', 'snapshotsListConnection', payload,null, headers)
            log.debug("LIST SNAPSHOTS FOR VIRTUAL MACHINE: ${rtn}")
            log.debug("DATA TYPE: ${rtn.data.getClass()}")
            if(rtn.success) {
                rtn.data.snapshots = rtn.data.snapshotsListConnection
            }
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
        ServiceResponse rtn = internalPostApiRequest(authConfig, null, 'vSphereVMAsyncRequestStatus', payload, null, headers)
        if(rtn.success) {
            rtn.data.request = rtn.data.vSphereVMAsyncRequestStatus?[0]
        }
        return rtn
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
        return internalPostApiRequest(authConfig, 'nodes', 'vSphereVmNewConnection', payload,null, headers)
    }

    ServiceResponse deleteSnapshot(authConfig, snapshotId) {
        String query = RubrikVmwareGqlQueryConstants.deleteSnapshot.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "deleteSnapshot",
                "variables": [
                        "snapshotIds": [ snapshotId ]
                ]
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, null, 'deleteUnmanagedSnapshots', payload,null, headers)
    }

    ServiceResponse listVCenterServers(Map authConfig) {
        String query = RubrikVmwareGqlQueryConstants.listVCenterServers.replaceAll("[\\r\\n]", "")
        def payload = [
                "query": query,
                "operationName": "listVCenterServers"
        ]
        def headers = ["Content-Type": "application/json"]
        return internalPostApiRequest(authConfig, 'nodes', 'vSphereVCenterConnection', payload, null, headers)
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
        return internalPostApiRequest(authConfig, null, 'refreshVsphereVcenter', payload,null, headers)
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
            if(vmIdResponse.success && vmIdResponse.data?.id) {
                log.debug("Virtual Machine now available in Rubrik")
                rtn.success = true
                rtn.data = [virtualMachine: [rubrikFid: vmIdResponse.data.id, clusterId: vmIdResponse.data.cluster.id]]
                keepGoing = false
            } else {
                if(attempt == 0) {
                    log.debug("virtual machine not found in Rubrik, initiating vCenter server refresh.")
                    // on first retry kick refresh vcenter servers
                    // if the vm isn't found in Rubrik the next refresh may not be for another 10 minutes,
                    // so kick off a manual refresh
                    new VcenterServerService(morpheusContext).executeRefresh(backupProvider, authConfig)
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
                        String cdmId = vmDetailResults.data.vSphereDetailData?[0].cdmId
                        String moid = "vm-" + cdmId?.split("vm-")[1]
                        log.debug("EXTRACTED MOID: ${moid}")
                        rtn.data = [virtualMachine: [ id: moid ]]
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
