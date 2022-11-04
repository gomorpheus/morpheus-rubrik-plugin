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
import com.morpheusdata.rubrik.vmware.services.RubrikVmwareApiService
import com.morpheusdata.rubrik.util.RubrikBackupStatusUtility
import groovy.util.logging.Slf4j

import java.lang.invoke.SerializedLambda;

@Slf4j
class RubrikVmwareBackupRestoreProvider implements BackupRestoreProvider {

	static String LOCK_NAME = "backups.rubrik.restore";

	Plugin plugin
	RubrikVmwareApiService apiService

	RubrikVmwareBackupRestoreProvider(Plugin plugin) {
		this.plugin = plugin
		this.apiService = new RubrikVmwareApiService()
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
			log.info("getBackupRestoreInstanceConfig: {}", backupResult)
			log.debug("restoreConfig: {}", restoreConfig)
			log.debug("opts: {}", opts)
			restoreConfig.config = backupResult.getConfigMap() ?: [:]
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
			def authConfig = apiService.getAuthConfig(backupProvider)
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
					ServiceResponse vmIdResults = apiService.waitForVirtualMachine(authConfig, sourceServer.externalId, backupProvider)
					log.debug("vmIdResults: ${vmIdResults}")
					if(vmIdResults.success && vmIdResults.data.virtualMachine.id) {
						def vmDetailResult = apiService.getVirtualMachine(authConfig, vmIdResults.data.virtualMachine.id)
						if(vmDetailResult.success) {
							def hostId = vmDetailResult.data.virtualMachine.hostId
							def vmHost = apiService.getHost(authConfig, hostId)
							if(vmHost.success) {
								def datastore = vmHost.data.host.datastores.find { it.name == sourceDatastore?.name }
								if(datastore) {
									def restoreOpts = [
										datastoreId: datastore.id,
										hostId     : hostId,
										vmName     : targetWorkload.name
									]
									ServiceResponse restoreResults = apiService.restoreSnapshotToNewVirtualMachine(authConfig, backupResult.externalId, restoreOpts)

									if(restoreResults.success) {
										ServiceResponse restoreTaskResults = apiService.waitForRestoredVirtualMachine(authConfig, restoreResults.data.restoreRequest.id)
										log.debug("wait for restore vm restults: ${restoreTaskResults}")
										if(restoreTaskResults.success && restoreTaskResults.data.virtualMachine?.id) {
											rtn.data.restoreConfig = [cloneVmId: restoreTaskResults.data.virtualMachine?.id]
											rtn.success = true
										} else {
											rtn.success = false
											rtn.msg = "Failed to restore virtual machine"
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
			} else {
				log.debug("Restoring to existing VM with snapshot ID: ${backupResult.externalId}")
				// restore to the current virtual machine
				ServiceResponse restoreResults = apiService.restoreSnapshotToVirtualMachine(authConfig, backupResult.externalId)
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
			BackupProvider backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)

			String restoreRequestId = backupRestore.externalStatusRef
			ServiceResponse restoreRequestResult = apiService.getVmTaskRequest(authConfig, restoreRequestId)
			Map restoreRequest = restoreRequestResult.data.request

			Long targetWorkloadId = backupRestore?.containerId
			Workload targetWorkload = plugin.morpheus.workload.get(targetWorkloadId)

			plugin.morpheus.instance.

			log.debug("restoreSession: ${restoreRequest}")
			if(restoreRequest) {
				if(restoreRequest.status == "SUCCEEDED") {
					def resultLink = restoreRequest.links.find { it.rel == "result" }
					if(resultLink) {
						def restoreResultId = apiService.extractUuid(resultLink.href)

						def vmDetailResults = apiService.getRestoredVirtualMachine(authConfig, restoreResultId)
						if(vmDetailResults.success && !vmDetailResults.retry) {
							rtn.data.backupRestore.externalId = vmDetailResults.data.moid // might need to get the VM info from the restore result links
						} else if(vmDetailResults.success && vmDetailResults.retry) {
							restoreRequest.status = MorpheusBackupStatusUtility.IN_PROGRESS
						} else {
							restoreRequest.status = MorpheusBackupStatusUtility.FAILED
						}
					}
				}
				rtn.data.backupRestore.status = RubrikBackupStatusUtility.getBackupStatus(restoreRequest.status) ?: backupRestore.status
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
				if(rtn.data.backupRestore.status == MorpheusBackupStatusUtility.FAILED) {
					rtn.data.backupRestore.errorMessage = restoreRequest.error.message
				}

				updateRestoredInstanceStatusFromRestoreStatus(rtn.data.backupRestore, targetWorkload.instance, targetWorkload)
				if(rtn.data.backupRestore.externalId && rtn.data.backupRestore.status == MorpheusBackupStatusUtility.SUCCEEDED) {
					finalizeRestore(backupRestore, targetWorkload.instance, targetWorkload)
				}
			}
		} catch(Exception ex) {
			log.error("refreshBackupRestoreResult error", ex)
		}

		return rtn
	}

	private finalizeRestore(BackupRestore backupRestore, Instance instance, Workload workload) {
		log.info("finalizeRestore: {}", backupRestore)
		 try {
			 // Need to update the externalId as it has changed
			 instance = workload?.instance
			 ComputeServer server = workload.server
			 ComputeServer existingServer = ComputeServer.where { account == server.account && zone == server.zone && externalId == backupRestore.externalId }.find()
			 def restoreToNew = backupRestore.getConfigProperty("restoreType") == "new"
			 if(existingServer && !restoreToNew) {
				 // the new server has already synced in, replace the server
				 workload.server = existingServer
				 plugin.morpheus.workload.save(workload)
			 } else {
				 // update the existing server with the new info
				 def vmwareInfo = vmwareComputeService.getServerDetail([zone: server.zone, externalId: backupRestore.externalId])
				 if(vmwareInfo.success) {
					 server.uniqueId = vmwareInfo.results.server.uuid
					 server.internalId = vmwareInfo.results.server.instanceUuid
					 server.externalId = vmwareInfo.results.server.mor
					 server.status = 'running'
					 def serverIp = vmwareInfo.results.server.ipAddress
					 if(serverIp != server.externalIp) {
						 if(server.externalIp == server.sshHost) {
							 server.sshHost = serverIp
						 }
						 server.externalIp = serverIp
					 }
					 if(serverIp != server.internalIp) {
						 if(server.internalIp == server.sshHost) {
							 server.sshHost = serverIp
						 }
						 server.internalIp = serverIp
					 }
				 }
				 server.save(flush:true)
				 workload.status = Container.Status.running
				 workload.save(flush:true)
			 }

			 // move vm
			 if(!restoreToNew) {
				 def rootVolume = server.volumes.find { it.rootVolume }
				 def datastore = rootVolume?.datastore
				 vmwareComputeService.relocateStorage(workload.server.zone, workload.server.externalId, datastore.externalId)
			 }

			 if(instance.layout.postProvisionService) {
				 // make sure to fail if post provision fails
				 try {
					 def postProvisionService = grailsApplication.mainContext[instance.layout.postProvisionService]
					 postProvisionService."${instance.layout.postProvisionOperation}"(instance)
				 } catch(Throwable t) {
					 log.error(t.message, t)
					 instance.refresh()
					 instance.status = Instance.Status.failed
					 instance.statusDate = new Date()
					 instance.save(flush: true)
					 throw t
				 }
			 }
			 // setup checks
			 monitorCheckManagementService.updateChecksFromInstance(instance)
			 // notify completion of this instance provisioning
			 sendRabbitMessage('main', 'instance.event', 'event.instance.provisioned', [instanceId:instance.id])
			 //restore an online backup
		 } catch(e) {
			 log.error("Error in finalizeRestore: ${e}", e)
			 instance?.status = Instance.Status.failed
			 instance?.save(flush:true)
		 }

		 return ServiceResponse.success()
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
				plugin.morpheus.instance.save(instance)
			}
		} catch (e) {
			log.error("Error updating instance status: ${e}", e)
		}
	}
}
