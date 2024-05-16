package com.morpheusdata.rubrik.vmware;

import com.morpheusdata.core.backup.util.BackupStatusUtility as MorpheusBackupStatusUtility
import com.morpheusdata.core.Plugin;
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.core.backup.response.BackupRestoreResponse
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Workload
import com.morpheusdata.model.projection.DatastoreIdentityProjection;
import com.morpheusdata.response.ServiceResponse;
import com.morpheusdata.model.BackupRestore;
import com.morpheusdata.model.BackupResult;
import com.morpheusdata.model.Backup;
import com.morpheusdata.model.Instance
import com.morpheusdata.rubrik.vmware.services.RubrikVmwareApiRestService
import com.morpheusdata.rubrik.vmware.services.RubrikVmwareApiGqlService
import com.morpheusdata.rubrik.util.RubrikBackupStatusUtility
import groovy.util.logging.Slf4j

@Slf4j
class RubrikVmwareBackupRestoreProvider implements BackupRestoreProvider {

	static String LOCK_NAME = "backups.rubrik.restore";

	Plugin plugin
	RubrikVmwareApiRestService apiRestService
	RubrikVmwareApiGqlService apiGqlService

	RubrikVmwareBackupRestoreProvider(Plugin plugin) {
		this.plugin = plugin
		this.apiRestService = new RubrikVmwareApiRestService()
		this.apiGqlService = new RubrikVmwareApiGqlService()
	}

	@Override
	ServiceResponse configureRestoreBackup(BackupResult backupResult, Map config, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse getBackupRestoreInstanceConfig(BackupResult backupResult, Instance instanceModel, Map restoreConfig, Map opts) {
		def rtn = [success:false, data:[:]]
		try {
			def backup = backupResult.backup
			log.debug("getBackupRestoreInstanceConfig: {}", backupResult)
			log.debug("restoreConfig: {}", restoreConfig)
			log.debug("opts: {}", opts)
			restoreConfig.config = backupResult.getConfigMap() ?: [:]

			// remove backupSetId from here so morpheus doesn't try to do a restore to existing from backup
			// during our temp vm extract restore process.
			if(restoreConfig?.instanceOpts?.provisionOpts?.backupSetId){
				restoreConfig?.instanceOpts?.provisionOpts?.remove('backupSetId')
			}
			if(opts.extractResults) {
				restoreConfig.instanceOpts = restoreConfig.instanceOpts ?: [:]
				restoreConfig.instanceOpts.provisionOpts = [
					cloneVmId: opts.extractResults.cloneVmId,
					cloneServerId: backupResult.serverId ?: backup.computeServerId
				]

				rtn.data = restoreConfig
				rtn.success = true
			} else {
				rtn.msg = "Unable to restore virtual machine, no source virtual machine ID found."
				rtn.success = false
			}


		} catch(e) {
			log.error("getBackupRestoreInstanceConfig error: ${e}", e)
		}
		return ServiceResponse.create(rtn)
	}

	@Override
	ServiceResponse validateRestoreBackup(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse getRestoreOptions(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse restoreBackup(BackupRestore backupRestore, BackupResult backupResult, Backup backup, Map opts) {
		ServiceResponse rtn = ServiceResponse.prepare(new BackupRestoreResponse(backupRestore))
		log.info("Restoring backupResult {} - opts: {}", backupResult, opts)
		try {
			BackupProvider backupProvider = backup.backupProvider
			def authConfig = backupProvider.platform == "rsc" ? apiGqlService.getAuthConfig(backupProvider) : apiRestService.getAuthConfig(backupProvider)
			log.debug("authConfig: ${authConfig}")
			log.debug("backup restore to new: ${backupRestore.restoreToNew}")
			if(backupRestore.restoreToNew) {
				log.debug("restoring to new VM")
				// restore to a new virtual machine
				def sourceWorkload = plugin.morpheus.workload.get(backupResult.containerId).blockingGet()
				ComputeServer sourceServer = sourceWorkload?.server
				DatastoreIdentityProjection sourceDatastore = sourceServer?.volumes?.find { it.rootVolume }?.datastore

				def targetWorkloadId = backupRestore?.containerId
				def targetWorkload = plugin.morpheus.workload.get(targetWorkloadId).blockingGet()

				if(sourceServer && sourceDatastore) {
					log.debug("Source server ext ID: ${sourceServer.externalId}")
					ServiceResponse vmIdResults = backupProvider.platform == "rsc" ? apiGqlService.waitForVirtualMachine(authConfig, sourceServer.externalId, backupProvider) : apiRestService.waitForVirtualMachine(authConfig, sourceServer.externalId, backupProvider)
					log.debug("vmIdResults: ${vmIdResults}")
					if(vmIdResults.success && vmIdResults.data.virtualMachine.id) {
						def vmDetailResult = backupProvider.platform == "rsc" ? apiGqlService.getVirtualMachine(authConfig, vmIdResults.data.virtualMachine.id) : apiRestService.getVirtualMachine(authConfig, vmIdResults.data.virtualMachine.id)
						if(vmDetailResult.success) {
							def hostId = vmDetailResult.data.virtualMachine.hostId
							def vmHost = backupProvider.platform == "rsc" ? apiGqlService.getHost(authConfig, hostId) : apiRestService.getHost(authConfig, hostId)
							if(vmHost.success) {
								log.debug("Source datastore, name: ${sourceDatastore.name} - id: ${sourceDatastore.id} - externalId: ${sourceDatastore.externalId}")
								def datastore = vmHost.data.host.datastores.find {
									log.debug("host datastore: ${it}")
									return it.id.endsWith(sourceDatastore.externalId)
								}
								if(datastore) {
									def restoreOpts = [
										datastoreId: datastore.id,
										hostId     : hostId,
										vmName     : targetWorkload.server?.name ?: targetWorkload.internalName
									]
									ServiceResponse restoreResults = backupProvider.platform == "rsc" ? apiGqlService.restoreSnapshotToNewVirtualMachine(authConfig, backupResult.externalId, vmIdResults.data.virtualMachine.id, restoreOpts) : apiRestService.restoreSnapshotToNewVirtualMachine(authConfig, backupResult.externalId, vmIdResults.data.virtualMachine.id, restoreOpts)
									log.debug("restoreResults: ${restoreResults}")
									if(restoreResults.success) {
										ServiceResponse restoreTaskResults = backupProvider.platform == "rsc" ? apiGqlService.waitForRestoredVirtualMachine(authConfig, restoreResults.data.restoreRequest.id) : apiRestService.waitForRestoredVirtualMachine(authConfig, restoreResults.data.restoreRequest.id)
										log.debug("wait for restore vm restults: ${restoreTaskResults}")
										if(restoreTaskResults.success && restoreTaskResults.data.virtualMachine?.id) {
											rtn.data.restoreConfig = [cloneVmId: restoreTaskResults.data.virtualMachine?.id]
											rtn.success = true
										} else {
											rtn.success = false
											rtn.msg = restoreTaskResults.msg ?: "Failed to restore virtual machine"
										}


										rtn.data.updates = true
										rtn.data.backupRestore.externalStatusRef = restoreResults.data.restoreRequest.id
										rtn.data.backupRestore.containerId = targetWorkload.id
										rtn.data.backupRestore.setConfigProperty("restoreType", "new")
									} else {
										rtn.success = false
										rtn.msg = "Unable to restore backup: ${restoreResults.msg ?: "Failed to initiate backup restore task"}"
									}
								} else {
									rtn.success = false
									rtn.msg = "Unable to determine target datastore."
								}
							} else {
								rtn.success = false
								rtn.msg = "Unable to find the target host on the service provider."
							}
						} else {
							rtn.success = false
							rtn.msg = "Unable to find target vm on the service provider"
						}
					} else {
						rtn.success = false
						rtn.msg = "Unable to find target vm on the service provider"
					}
				} else {
					rtn.success = false
					rtn.msg = "Unable to determine source datastore"
				}

				if(rtn.success == false) {
					rtn.data.updates = true
					rtn.data.backupRestore.status = MorpheusBackupStatusUtility.FAILED
					if(rtn.data.backupRestore.errorMessage == null && rtn.msg) {
						rtn.data.backupRestore.errorMessage = rtn.msg
					}

				}
			} else {
				log.debug("Restoring to existing VM with snapshot ID: ${backupResult.externalId}")
				// restore to the current virtual machine
				ServiceResponse vmIdResults = backupProvider.platform == "rsc" ? apiGqlService.waitForVirtualMachine(authConfig, backupResult.externalId, backupProvider) : apiRestService.waitForVirtualMachine(authConfig, backupResult.externalId, backupProvider)
				ServiceResponse restoreResults = backupProvider.platform == "rsc" ? apiGqlService.restoreSnapshotToVirtualMachine(authConfig, backupResult.externalId, vmIdResults.data.virtualMachine.id) : apiRestService.restoreSnapshotToVirtualMachine(authConfig, backupResult.externalId, vmIdResults.data.virtualMachine.id)
				if(restoreResults.success) {
					rtn.success = true
					rtn.data.updates = true
					rtn.data.backupRestore.externalStatusRef = restoreResults.data.restoreRequest.id
					rtn.data.backupRestore.setConfigProperty("restoreType", "existing")
				} else {
					rtn.success = false
					rtn.msg = "Unable to restore backup: ${restoreResults.msg ?: "unknown api error"}"
				}
			}
			log.debug("restoreBackup result: {}", rtn)

		} catch(e) {
			log.error("restoreBackup error", e)
			rtn.error = "Failed to restore backup: ${e}"
		}

		return rtn
	}

	@Override
	ServiceResponse refreshBackupRestoreResult(BackupRestore backupRestore, BackupResult backupResult) {
		log.debug("refreshBackupRestoreResult: backupResult:${backupResult.id}, restore:${backupRestore.id}")
		ServiceResponse<BackupRestoreResponse> rtn = ServiceResponse.prepare(new BackupRestoreResponse(backupRestore))
		try{
			log.debug("Restore status: status:${backupRestore.status}, errorMessage: ${backupRestore.errorMessage}")
			Backup backup = backupResult.backup
			BackupProvider backupProvider = backup.backupProvider
			def authConfig = backupProvider.platform == "rsc" ? apiGqlService.getAuthConfig(backupProvider) : apiRestService.getAuthConfig(backupProvider)
			String restoreRequestId = backupRestore.externalStatusRef
			log.debug("restore request id: {}", restoreRequestId)
			if(restoreRequestId) {
				ServiceResponse restoreRequestResult = backupProvider.platform == "rsc" ? apiGqlService.getVmTaskRequest(authConfig, restoreRequestId) : apiRestService.getVmTaskRequest(authConfig, restoreRequestId)
				log.debug("restoreRequestResult: {}", restoreRequestResult)
				Map restoreRequest = restoreRequestResult.data.request

				Long targetWorkloadId = backupRestore?.containerId
				Workload targetWorkload = plugin.morpheus.workload.get(targetWorkloadId).blockingGet()

				log.debug("restoreSession: ${restoreRequest}")
				if(restoreRequest) {
					if(restoreRequest.status == "SUCCEEDED") {
						def resultLink = restoreRequest.links.find { it.rel == "result" }
						if(resultLink) {
							def restoreResultId =  backupProvider.platform == "rsc" ? apiGqlService.extractUuid(resultLink.href) : apiRestService.extractUuid(resultLink.href)

							def vmDetailResults =  backupProvider.platform == "rsc" ? apiGqlService.getRestoredVirtualMachine(authConfig, restoreResultId) : apiRestService.getRestoredVirtualMachine(authConfig, restoreResultId)
							if(vmDetailResults.success && !vmDetailResults.data.retry) {
								rtn.data.backupRestore.externalId = vmDetailResults.data.moid // might need to get the VM info from the restore result links
							} else if(vmDetailResults.success && vmDetailResults.retry) {
								restoreRequest.status = MorpheusBackupStatusUtility.IN_PROGRESS
							} else {
								restoreRequest.status = MorpheusBackupStatusUtility.FAILED
							}
						}
					}
					rtn.data.backupRestore.status = RubrikBackupStatusUtility.getBackupStatus(restoreRequest.status) ?: backupRestore.status
					log.debug("Backup restore status: ${rtn.data.backupRestore.status}")
					Date startDate = DateUtility.parseDate(restoreRequest.startTime)
					Date endDate = DateUtility.parseDate(restoreRequest.endTime)
					rtn.data.backupRestore.startDate = startDate
					rtn.data.backupRestore.lastUpdated = new Date()
					if(startDate && endDate) {
						Long start = startDate?.getTime()
						Long end = endDate?.getTime()
						rtn.data.backupRestore.endDate = end ? new Date(end) : null
						rtn.data.backupRestore.duration = (start && end) ? (end - start) : 0
					}
					if(rtn.data.backupRestore.status == MorpheusBackupStatusUtility.FAILED && restoreRequest.error?.message) {
						rtn.data.backupRestore.errorMessage = restoreRequest.error.message
					}

					updateRestoredInstanceStatusFromRestoreStatus(rtn.data.backupRestore, targetWorkload.instance, targetWorkload)
					plugin.morpheus.backup.backupRestore.save(rtn.data.backupRestore).blockingGet()
				} else {
					rtn.data.backupRestore.status = MorpheusBackupStatusUtility.FAILED
					rtn.data.backupRestore.errorMessage = "No restore request found."
					plugin.morpheus.backup.backupRestore.save(rtn.data.backupRestore).blockingGet()
				}
			}
		} catch(Exception ex) {
			log.error("refreshBackupRestoreResult error", ex)
		}

		return rtn
	}

	private updateRestoredInstanceStatusFromRestoreStatus(BackupRestore backupRestore, Instance instance, Workload workload) {
		try {
			boolean doSave = false
			if(backupRestore.status == MorpheusBackupStatusUtility.SUCCEEDED) {
				instance?.status = Instance.Status.running
				doSave = true
			} else if(backupRestore.status == MorpheusBackupStatusUtility.FAILED) {
				if(!workload?.server?.externalId) {
					instance?.status = Instance.Status.failed
				} else {
					instance?.status = Instance.Status.unknown
				}
				doSave = true
			} else if(backupRestore.status == MorpheusBackupStatusUtility.IN_PROGRESS) {
				instance?.status = Instance.Status.restoring
				doSave = true
			}

			if(doSave) {
				plugin.morpheus.instance.save([instance])
			}
		} catch (e) {
			log.error("Error updating instance status: ${e}", e)
		}
	}
}
