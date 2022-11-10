package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.util.RestApiUtil
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.ApiService
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareApiService extends ApiService {

	ServiceResponse listVirtualMachines(Map authConfig) {
		return internalGetApiRequest(authConfig, '/vmware/vm', 'virtualMachines')
	}

	ServiceResponse getVirtualMachine(Map authConfig, String vmId) {
		return internalGetApiRequest(authConfig, '/vmware/vm/' + vmId, 'virtualMachine')
	}

	ServiceResponse getVirtualMachineId(Map authConfig, String vmExternalId) {
		def query = [moid: vmExternalId]
		return internalGetApiRequest(authConfig, '/vmware/vm', 'virtualMachine', query)
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
				rtn = internalPatchApiRequest(authConfig, '/vmware/vm/' + vmId, 'virtualMachine', vmOpts)
			} else {
				rtn.msg = "VM not found in Rubrik."
			}
		} catch(e) {
			log.error("error updating virtual machine: {}", e, e)
		}
		return rtn
	}

	ServiceResponse backupVirtualMachine(Map authConfig, String vmExternalId, Map opts = [:]) {
		return internalPostApiRequest(authConfig, '/vmware/vm/' + vmExternalId + '/snapshot', 'backupRequest')
	}

	ServiceResponse restoreSnapshotToVirtualMachine(Map authConfig, String snapshotId, Map opts=[:]) {
		Map body = [
			config: [
				powerOn: true,
				disableNetwork: false,
				preserveMoid: true
			]
		]
		return internalPostApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId + '/instant_recover', 'restoreRequest', body)
	}

	ServiceResponse restoreSnapshotToNewVirtualMachine(Map authConfig, String snapshotId, Map opts=[:]) {
		Map body = [
			powerOn: true,
			disableNetwork: false,
			datastoreId: opts.datastoreId,
			hostId: opts.hostId,
			vmName: opts.vmName
		]
		return internalPostApiRequest(authConfig, '/vmware/vm/snapshot/' + snapshotId + '/export', 'restoreRequest', body)
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
		return internalGetApiRequest(authConfig, '/vmware/vm/snapshot/mount/' + mountId, 'mount')
	}

	@Override
	ServiceResponse listHosts(Map authConfig) {
		return internalGetApiRequest(authConfig, '/vmware/host', 'hosts')
	}

	ServiceResponse getHost(Map authConfig, String hostId) {
		return internalGetApiRequest(authConfig, '/vmware/host/' + hostId, 'host')
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
				rtn = internalGetApiRequest(authConfig, '/vmware/vm/' + vmId + '/snapshot', 'snapshots')
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
