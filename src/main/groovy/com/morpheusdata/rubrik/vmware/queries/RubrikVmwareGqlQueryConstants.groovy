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
                    numWorkloadDescendants
                }
            }
        }
    """

    static final getVirtualMachine = """
        query getVirtualMachine (\$id: [String!]) {
            vSphereDetailData: vSphereVmNew(fid: \$id) {
                id
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
                onDemandSnapshotCount
            }
        }

    """

    static final getVirtualMachineId = """
        query getVirtualMachineId(\$moid: [String!]) {
            vSphereVmNewConnection(filter: {field: NAME, texts: \$moid}) {
                count
                nodes {
                    cdmId
                    id
                    name
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
        mutation updateVirtualMachine (\$id: String!, \$configuredSlaDomainId: String ) {
            updateVsphereVm (input: {
                id: \$id,
                vmUpdateProperties: {
                    virtualMachineUpdate: {
                        configuredSlaDomainId: \$configuredSlaDomainId
                    }
                }
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
            }
        }
       """

    static final restoreSnapshotToVirtualMachine = """
        mutation vSphereInstantRecoverMutation(\$id: String!, \$snapshotId: String, \$disableNetwork: Boolean, \$keepMacAddresses: Boolean, \$removeNetworkDevices: Boolean, \$preserveMoid: Boolean, \$powerOn: Boolean) {
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
            }
        }
    """

    static final restoreSnapshotToNewVirtualMachine = """
        mutation VSphereVmExportSnapshotV3Mutation(\$id: String!, \$snapshotId: String, \$hostId: String, \$disableNetwork: Boolean, \$powerOn: Boolean, \$vmName: String, \$storageLocationId: String) {
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

    static final getMount = """
        query getMount (\$fid: UUID!){
            vSphereMount (fid: \$fid) {
                id
                cdmId
                isReady
                status
                cluster {
                    id
                    name
                }
                host {
                    id
                    name
                }
                sourceVm {
                    name
                    id
                }
                newVm {
                    name
                    id
                }
                sourceSnapshot {
                    id
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
                numWorkloadDescendants
            }
        }
    """

    static final listSnapshotsForVirtualMachine = """
        query SnapshotsListSingleQuery(\$workloadId: String!) {
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

    static final deleteSnapshot = """
        mutation deleteSnapshot (\$location: DeleteVmwareSnapshotRequestLocation!, \$id: String!) {
            vsphereVmDeleteSnapshot(input: {
                location: \$location
                id: \$id
            })
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
            refreshVsphereVcenter (input: {
                fid: \$fid
            }) {
                id
                status
            }
        }
    """

    static final getHostId = """
        query getHostId(\$fid: UUID!) {
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