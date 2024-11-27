package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.RestApiService
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareRestApiService extends RestApiService implements RubrikVmwarePlatformApiServiceInterface {
	private MorpheusContext morpheusContext
	RubrikVmwareRestApiService(MorpheusContext morpheusContext) {
		super(morpheusContext)
		this.morpheusContext = morpheusContext
	}

	ServiceResponse listVirtualMachines(Map authConfig) {
		return internalGetApiRequest(authConfig, '/vmware/vm', 'virtualMachines')
	}

	ServiceResponse getVirtualMachine(Map authConfig, String vmId) {
		return internalGetApiRequest(authConfig, '/vmware/vm/' + vmId, 'virtualMachine')
	}

	ServiceResponse getVirtualMachineByMoid(Map authConfig, String vmExternalId, String hostExternalId) {
		def query = [moid: vmExternalId]
		return internalGetApiRequest(authConfig, '/vmware/vm', 'virtualMachine', query)
	}


	ServiceResponse updateVirtualMachine(Map authConfig, String vmId, Map vmOpts, Map opts = [:]) {
		ServiceResponse rtn = ServiceResponse.prepare()
		log.info("UPDATE VIRTUAL MACHINE VM ID: ${vmId}")
		try {
			rtn = internalPatchApiRequest(authConfig, '/vmware/vm/' + vmId, 'virtualMachine', vmOpts)
		} catch(e) {
			log.error("error updating virtual machine: {}", e, e)
		}
		return rtn
	}

	ServiceResponse backupVirtualMachine(Map authConfig, String vmId, Map opts = [:]) {
		return internalPostApiRequest(authConfig, '/vmware/vm/' + vmId + '/snapshot', 'backupRequest')
	}

	ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
		Map body = [
			powerOn: true,
			disableNetwork: false,
			preserveMoid: true,
			keepMacAddresses: true,
			removeNetworkDevices: false,
		]
		log.debug("restoreSnapshotToVirtualMachine, snapshotId: {}, body: {}", snapshotId, body)
		return internalPostApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId + '/instant_recover', 'restoreRequest', body)
	}

	ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, String vmId, Map opts=[:]) {
		Map body = [
			powerOn: true,
			disableNetwork: false,
			datastoreId: opts.datastoreId,
			hostId: opts.hostId,
			vmName: opts.vmName
		]
		log.debug("restoreSnapshotToNewVirtualMachine, snapshotId: {}, body: {}", snapshotId, body)
		return internalPostApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId + '/export', 'restoreRequest', body)
	}

	ServiceResponse getMount(Map authConfig, String mountId) {
		return internalGetApiRequest(authConfig, '/vmware/vm/snapshot/mount/' + mountId, 'mount')
	}

	@Override
	ServiceResponse listHosts(Map authConfig) {
		return internalGetApiRequest(authConfig, '/vmware/host', 'hosts')
	}

	ServiceResponse getHost(Map authConfig, String hostId) {
		return internalGetApiRequest(authConfig, '/vmware/host/' + hostId, 'host')
	}

	ServiceResponse listSnapshotsForVirtualMachine(Map authConfig, vmId) {
		def rtn = ServiceResponse.prepare()
		try {
			rtn = internalGetApiRequest(authConfig, '/vmware/vm/' + vmId + '/snapshot', 'snapshots')
		} catch(e) {
			log.error("error listing snapshots: ${e}", e)
		}
		return rtn
	}

	// was `getRequest`
	ServiceResponse getVmTaskRequest(Map authConfig, String clusterId, String requestId) {
		return internalGetApiRequest(authConfig, '/vmware/vm/request/' + requestId, 'request')
	}

	ServiceResponse getSnapshot(authConfig, snapshotId) {
		return internalGetApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId, 'snapshot')
	}

	ServiceResponse deleteSnapshot(authConfig, snapshotId) {
		Map queryParams = [location: "all"]
		return internalDeleteApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId, queryParams)
	}

	ServiceResponse listVCenterServers(Map authConfig) {
		return internalGetApiRequest(authConfig, '/vmware/vcenter', 'vcenterServers')
	}

	ServiceResponse refreshVcenterServer(Map authConfig, String serverId) {
		return internalPostApiRequest(authConfig, '/vmware/vcenter/' + serverId + '/refresh', 'request')
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
				rtn.data = [virtualMachine: [rubrikFid: vmIdResponse.data.virtualMachine?.getAt(0)?.id]]
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
