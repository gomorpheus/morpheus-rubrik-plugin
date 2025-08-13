package com.morpheusdata.rubrik.vmware.queries
import groovy.transform.CompileStatic

@CompileStatic
class RubrikVmwareGqlQueryConstants {
    static final listVirtualMachines = """
        query listVirtualMachines {
            vSphereVmNewConnection {
                count
                nodes {
                    cdmId
                    id
                    name
                    cluster {
                        name
                        id
                    }
                    effectiveSlaDomain {
                        name
                        id
                    }
                }
            }
        }
    """

    static final getVirtualMachine = """
        query getVirtualMachine (\$fid: UUID!) {
            vSphereDetailData: vSphereVmNew(fid: \$fid) {
                id
                cdmId
                name
                effectiveSlaDomain {
                    id
                    name
                }
                slaPauseStatus
                cluster {
                    id
                    name
                }
                totalSnapshots: snapshotConnection {
                    count
                }
                physicalPath {
                    fid
                    name
                    objectType
                }
                snapshotConnection {
                    nodes {
                        id
                        cdmId
                        cluster {
                            id
                            name
                        }
                        isOnDemandSnapshot
                    }
                }
            }
        }

    """

    static final getVirtualMachineByMoid = """
        query getVirtualMachineByMoid(\$moid: [String!]) {
            vSphereVmNewConnection(filter: {field: VMWARE_VM_MOID, texts: \$moid}) {
                count
                nodes {
                    cdmId
                    id
                    name
                    effectiveSlaDomain {
                        id
                        name
                    }
                    cluster {
                        id
                    }
                    physicalPath {
                        fid
                        name
                        objectType
                    }
                }
            }
        } 
    """

    static final updateVirtualMachine = """
        mutation updateVirtualMachine(\$slaDomainAssignType: SlaAssignTypeEnum!, \$slaOptionalId: UUID, \$objectIds: [UUID!]!) {
            assignSla(input: {
                slaDomainAssignType: \$slaDomainAssignType,
                slaOptionalId: \$slaOptionalId,
                objectIds: \$objectIds
            }) {
                success
            }
        }
    """

    static final backupVirtualMachine = """
        mutation backupVirtualMachine (\$slaId: String, \$id: String!) {
            vsphereOnDemandSnapshot (input: {
                config: {
                    slaId: \$slaId
                },
                id: \$id,
                userNote: ""
            }) {
                id
                status
                startTime
                progress
                error {
                    message: message
                }
            }
        }
       """

    static final restoreSnapshotToVirtualMachine = """
        mutation restoreSnapshotToVirtualMachine(\$id: String!, \$snapshotId: String, \$disableNetwork: Boolean, \$keepMacAddresses: Boolean, \$removeNetworkDevices: Boolean, \$preserveMoid: Boolean, \$powerOn: Boolean) {
            vsphereVmInitiateInstantRecoveryV2(input: {
                id: \$id,
                config: {
                    requiredRecoveryParameters: {
                        snapshotId: \$snapshotId
                    },
                    mountExportSnapshotJobCommonOptionsV2: {
                        disableNetwork: \$disableNetwork
                        keepMacAddresses: \$keepMacAddresses,
                        powerOn: \$powerOn,
                        removeNetworkDevices: \$removeNetworkDevices
                    },
                    preserveMoid: \$preserveMoid
                }
            }) {
                status
                id
                startTime
                endTime
                progress
                error { 
                    message: message
                }
            }
        }
    """

    static final restoreSnapshotToNewVirtualMachine = """
        mutation restoreSnapshotToNewVirtualMachine(\$id: String!, \$snapshotId: String, \$hostId: String, \$disableNetwork: Boolean, \$powerOn: Boolean, \$vmName: String, \$storageLocationId: String) {
            vsphereVmExportSnapshotV3(input: {
                id: \$id,
                config: {
                    hostId: \$hostId,
                    storageLocationId: \$storageLocationId,
                    mountExportSnapshotJobCommonOptionsV2: {
                        disableNetwork: \$disableNetwork,
                        powerOn: \$powerOn,
                        vmName: \$vmName
                    },
                    requiredRecoveryParameters: {
                        snapshotId: \$snapshotId
                    }
                }
            }) {
                id
                status
            }
        }
    """

    static final getMounts = """
        query getMounts {
            vSphereMountConnection {
                nodes {
                    id
                    cdmId
                    isReady
                    status
                    cluster {
                        name
                        id
                    }
                    host {
                        name
                        id
                    }
                    newVm {
                        name
                        id
                    }
                    sourceVm {
                        name
                        id
                    }
                    sourceSnapshot {
                    id
                    }
                }
            }
        }
    """

    static final listHosts = """
        query listHosts {
            vSphereHostConnection {
                nodes {
                    cdmId
                    id
                    name
                    effectiveSlaDomain {
                        id
                        name
                    }
                    cluster {
                        id
                        name
                    }
                    numWorkloadDescendants
                }
            }
        }
    """

    static final getHost = """
        query getHost (\$fid: UUID!) {
            vSphereHost (fid: \$fid) {
                cdmId
                id
                name
                cluster {
                    id
                    name
                }
                effectiveSlaDomain {
                    id
                    name
                }
                descendantConnection (typeFilter: VSphereDatastore){
                  nodes {
                    id
                    name
                    objectType
                  }
                }
            }
        }
    """

    static final listSnapshotsForVirtualMachine = """
        query listSnapshotsForVirtualMachine(\$workloadId: String!) {
            snapshotsListConnection: snapshotOfASnappableConnection(workloadId: \$workloadId) {
                nodes {
                    id
                    date
                    expirationDate
                    isExpired
                    isOnDemandSnapshot
                    snappableId
                }
            }
        }
    """

    static final getVmTaskRequest = """
        query getVmTaskRequest (\$clusterUuid: UUID!, \$requestId: String!) {
            vSphereVMAsyncRequestStatus (clusterUuid:\$clusterUuid, id: \$requestId) {
                id
                status
                startTime
                endTime
                progress
                links {
                    href: href, 
                    rel: rel
                }
                error {
                    message: message
                }
            }
        }
    """

    static final getSnapshot = """
        query getSnapshot (\$snapshotId: [UUID!]) {
            vSphereVmNewConnection {
                nodes {
                    cdmId
                    id
                    name
                    snapshotConnection (filter: {
                        snapshotId: \$snapshotId
                    }) {
                        nodes {
                            id
                            cdmId
                            cluster {
                                id
                                name
                            }
                            isOnDemandSnapshot
                        }
                    }
                }
            }
        }
    """

//    static final deleteSnapshot = """
//        mutation deleteSnapshot (\$location: DeleteVmwareSnapshotRequestLocation!, \$id: String!) {
//            vsphereVmDeleteSnapshot(input: {
//                location: \$location
//                id: \$id
//            })
//        }
//    """

    static final deleteSnapshot = """
        mutation deleteSnapshot (\$snapshotIds: [UUID!]!) {
            deleteUnmanagedSnapshots(input: {
                snapshotIds: \$snapshotIds
            }) {
                success
            }
        }
    """

    static final listVCenterServers = """
        query listVCenterServers {
            vSphereVCenterConnection {
                nodes {
                    id
                    name
                    vcenterId
                    username
                    connectionStatus {
                        status
                        message
                    }
                    cluster {
                        id
                        name
                    }
                    effectiveSlaDomain {
                        id
                        name
                    }
                    numWorkloadDescendants
                }
            }
        }
    """

    static final refreshVcenterServer = """
        mutation refreshVcenterServer (\$fid: UUID!) {
            refreshVsphereVcenter (input: {fid: \$fid}) {
                id
                status
                error {
                    message
                    __typename
                }
                __typename
            }
        }
    """

    static final getHostById = """
        query getHostById(\$fid: UUID!) {
            vSphereHost(fid: \$fid) {
                id
                name
                cdmId
                cluster {
                    id
                }
            }
        }
    """
}