package com.morpheusdata.rubrik.vmware

import com.morpheusdata.core.Plugin;
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility as MoprheusBackupResultUtility
import com.morpheusdata.core.backup.util.BackupStatusUtility as MorpheusBackupStatusUtility
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.util.RubrikBackupStatusUtility
import com.morpheusdata.rubrik.vmware.services.RubrikVmwareApiService
import groovy.util.logging.Slf4j
import org.apache.tools.ant.types.spi.Service

@Slf4j
class RubrikVmwareBackupExecutionProvider implements BackupExecutionProvider {

	static String LOCK_NAME = "backups.rubrik.execution";

	Plugin plugin
	RubrikVmwareApiService apiService

	RubrikVmwareBackupExecutionProvider(Plugin plugin) {
		this.plugin = plugin
		this.apiService = new RubrikVmwareApiService()
	}

	@Override
	ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
		log.debug("configuring backup with properties {} and opts {}", config, opts)
		def slaDomain = config.config?.rubrikSlaDomain ?: opts.config?.rubrikSlaDomain
		log.debug("slaDomain: {}", slaDomain)
		if(slaDomain) {
			backup.setConfigProperty("rubrikSlaDomain", slaDomain)
		}

		return ServiceResponse.success(backup)
	}

	@Override
	ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	@Override
	ServiceResponse createBackup(Backup backup, Map opts) {
		log.debug("creating backup for backup {}:{} with opts: {}", backup.id, backup.name, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			// Only need to update the VM with an SLA Domain if necessary
			BackupProvider backupProvider = backup.backupProvider
			def slaDomainId = opts.rubrikSlaDomain ?: backup.getConfigProperty('rubrikSlaDomain')
			log.debug("slaDomainId: {}", slaDomainId)
			if(slaDomainId) {
				def authConfig = apiService.getAuthConfig(backupProvider)
				def morphServer = null
				if(backup.computeServerId) {
					morphServer = plugin.morpheus.computeServer.get(backup.computeServerId).blockingGet()
				}
				if(morphServer) {
					// wait for the vm details to show up in the rubrik api. This is most critical after the initial provision or after a clone.
					ServiceResponse vmIdResult = apiService.waitForVirtualMachine(authConfig, morphServer.externalId, backupProvider)
					log.debug("vmIdResult: ${vmIdResult}")
					if(vmIdResult.success && vmIdResult.data.virtualMachine?.id) {
						def slaDomain = plugin.morpheus.referenceData.get(slaDomainId.toLong()).blockingGet()
						// if we find the id, update the vm with the sla domain
						log.debug("morphServer.externalId: ${morphServer.externalId}, slaDomainID: ${slaDomain?.externalId}")
						rtn = apiService.updateVirtualMachine(authConfig, morphServer.externalId, [configuredSlaDomainId: slaDomain?.externalId])
					} else {
						rtn.success = false
						rtn.msg = "Unable to find vcenter virtual machine in Rubrik."
					}
				}
			} else {
				rtn.success = true
			}
		} catch(e) {
			log.error("createBackup error: ${e}", e)
		}
		return rtn
	}

	@Override
	ServiceResponse deleteBackup(Backup backup, Map opts) {
		log.debug("deleting backup {}", backup)
		def rtn = [success:false]
		try {
			def backupProvider = backup.backupProvider
			if(backupProvider) {
				def authConfig = apiService.getAuthConfig(backupProvider)

				if(backup.getConfigProperty("rubrikSlaDomain")) {
					def morphServer = null
					def morphServerId = opts.server?.id ?: backup.computeServerId
					if(morphServerId) {
						try {
							morphServer = plugin.morpheus.computeServer.get(morphServerId).blockingGet()
						} catch (Throwable t2) {
							// this is expected, the Single returned doesn't handle optional so we have
							// to catch the exception here if the backup had been retained from the
							// resources deleted.
						}
					}
					if(morphServer) {
						rtn = apiService.updateVirtualMachine(authConfig, morphServer.externalId, [configuredSlaDomainId: "INHERIT"]) // INHERIT or UNPROTECTED
						log.debug("deleteBackup API results: {}", rtn)
						if(!rtn.success && rtn.msg.contains("not found")) {
							rtn.success = true
						}
					} else {
						// this is a retained backup, allow delete to proceed
						rtn.success = true
					}
				} else {
					rtn.success = true
				}
			} else {
				//orphaned from backupprovider, just clean up
				rtn.success = true				
			}
			
		} catch (Throwable t) {
			log.error(t.message, t)
			throw new RuntimeException("Unable to remove backup:${t.message}", t)
		}
		return rtn
	}

	@Override
	ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		log.debug("deleting backup result {}", backupResult)
		ServiceResponse rtn = ServiceResponse.prepare()

		def backupProvider = backupResult.backup?.backupProvider
		if(backupProvider) {
			def authConfig = apiService.getAuthConfig(backupProvider)

			def resultConfig = backupResult.getConfigMap()
			def isOnDemandSnapshot = resultConfig.containsKey("isOnDemandSnapshot") ? resultConfig.isOnDemandSnapshot : true
			if(isOnDemandSnapshot) {
				if(backupResult.externalId) {
					ServiceResponse deleteResult = apiService.deleteSnapshot(authConfig, backupResult.externalId)
					log.debug("deleteResult erroCode: ${deleteResult.errorCode}")
					if(deleteResult.success || deleteResult.errorCode == "404") {
						rtn.success = true
					}
				} else {
					rtn.success = true
				}
			} else {
				log.debug("not on demand snapshot")
				// we can't delete SLA snapshots
				rtn.success = true
			}
		} else {
			//backup provider no longer exists on the entry, just clean up the record.
			rtn.success = true
		}
		

		log.debug("deleteBackupResult result: ${rtn}")
		return rtn
	}

	@Override
	ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
		log.debug("Executing backup {} with result {}", backup.id, backupResult.id)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		try {
			def backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)

			if(computeServer) {
				ServiceResponse vmIdResults = apiService.waitForVirtualMachine(authConfig, computeServer.externalId, backupProvider)
				if(vmIdResults.success && vmIdResults.data.virtualMachine?.id) {
					// disable cloud init and clear cache to force cloud init on restore
					if(computeServer.sourceImage && computeServer.sourceImage.isCloudInit && computeServer.serverOs?.platform != 'windows') {
						getPlugin().morpheus.executeCommandOnServer(computeServer, 'sudo rm -f /etc/cloud/cloud.cfg.d/99-manual-cache.cfg; sudo cp /etc/machine-id /tmp/machine-id-old ; sync', true, computeServer.sshUsername, computeServer.sshPassword, null, null, null, null, true, true).blockingGet()
					}

					String vmId = vmIdResults.data.virtualMachine?.id
					ServiceResponse backupRequestResult = apiService.backupVirtualMachine(authConfig, vmId)
					log.debug("executeBackup requestResult: {}", backupRequestResult)
					if(backupRequestResult.success == true) {
						rtn.data.backupResult.status = RubrikBackupStatusUtility.getBackupStatus(backupRequestResult.data.backupRequest?.status)
						if(backupRequestResult.data.backupRequest?.id){
							String requestId = backupRequestResult.data.backupRequest?.id
							rtn.data.backupResult.setConfigProperty("backupRequestId", requestId)
							if(backupRequestResult.data.backupRequest.startTime) {
								rtn.data.backupResult.startDate = DateUtility.parseDate(backupRequestResult.data.backupRequest.startTime)
							}
							rtn.data.updates = true
							rtn.success = true
						} else {
							rtn.success = false
							rtn.msg = "No "
						}

					} else {
						rtn.success = false
						rtn.msg = backupRequestResult.data.backupRequest.error ?: "failed to initialize backup snapshot"
					}
				} else {
					rtn.success = false
					rtn.msg = "Unable to find vcenter virtual machine in Rubrik."
					log.debug("Unable to find vcenter virtual machine in Rubrik.")
				}
			}
		} catch(Exception e) {
			rtn.success = false
			rtn.msg = e.getMessage()
			log.error("executeBackup error: ${e}", e)
		}
		return rtn
	}

	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		log.debug("refreshing backup result {}", backupResult.id)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		try {
			Backup backup = backupResult.backup
			BackupProvider backupProvider = backup.backupProvider
			Map authConfig = apiService.getAuthConfig(backupProvider)

			String snapshotId = null
			String requestId = backupResult.getConfigProperty('backupRequestId')
			if(requestId) {
				ServiceResponse requestResult = apiService.getVmTaskRequest(authConfig, requestId)
				Map requestDetail = requestResult.data.request
				if(!snapshotId && requestResult.success && requestDetail.status == RubrikBackupStatusUtility.STATUS_SUCCEEDED) {
					log.debug("snapshot created successfully, getting snapshot info for backup result")
					Map snapshotLink = requestDetail.links.find { it.rel == "result" }
					if(snapshotLink) {
						snapshotId = apiService.extractUuid(snapshotLink.href)
					}
				}

				if(requestResult.success) {
					log.debug("Updating backup result progress: {}", requestResult.content)
					boolean doUpdate = false

					if(!rtn.data.backupResult.externalId && snapshotId) {
						rtn.data.backupResult.externalId = snapshotId
						doUpdate = true
					}

					def updatedStatus = RubrikBackupStatusUtility.getBackupStatus(requestDetail?.status)
					if(rtn.data.backupResult.status != updatedStatus) {
						rtn.data.backupResult.status = updatedStatus
					}

					Date startDate = DateUtility.parseDate(requestDetail.startTime)
					Date endDate = requestDetail.endTime ? DateUtility.parseDate(requestDetail.endTime) : null
					if(startDate && rtn.data.backupResult.startDate != startDate) {
						rtn.data.backupResult.startDate = startDate
						doUpdate = true
					}
					if(endDate && rtn.data.backupResult.endDate != endDate) {
						rtn.data.backupResult.endDate = endDate
						doUpdate = true
					}
					if(startDate && endDate) {
						Long start = startDate.getTime()
						Long end = endDate.getTime()
						Long durationMillis = (start && end) ? (end - start) : 0
						if(rtn.data.backupResult.durationMillis != durationMillis) {
							rtn.data.backupResult.durationMillis = durationMillis
							doUpdate = true

						}
					}

					rtn.data.updates = doUpdate
					rtn.success = true

					// backup completed, re-enable cloud-init
					if([MorpheusBackupStatusUtility.FAILED, MorpheusBackupStatusUtility.CANCELLED, MorpheusBackupStatusUtility.SUCCEEDED].contains(updatedStatus)) {
						Long computeServerId = backupResult.serverId
						ComputeServer computeServer = getPlugin().morpheus.computeServer.get(computeServerId).blockingGet()
						if(computeServer && computeServer.sourceImage && computeServer.sourceImage.isCloudInit && computeServer.serverOs?.platform != 'windows') {
							getPlugin().morpheus.executeCommandOnServer(computeServer, "sudo bash -c \"echo 'manual_cache_clean: True' >> /etc/cloud/cloud.cfg.d/99-manual-cache.cfg\"; sudo cat /tmp/machine-id-old > /etc/machine-id ; sudo rm /tmp/machine-id-old ; sync", true, computeServer.sshUsername, computeServer.sshPassword, null, null, null, null, true, true).blockingGet()
						}
					}

				}
			} else {
				// backup isn't ready yet, wait for next refresh.
				rtn.success = true
			}
		} catch(Exception e) {
			rtn.success = false
			rtn.msg = e.getMessage()
			log.error("refreshBackupResult error: ${e}", e)
		}

		return rtn
	}

	@Override
	ServiceResponse cancelBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.error("Unable to cancel backup")
	}

	@Override
	ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.error("Unable to extract backups from a Rubrik backup provider")
	}

	// this is probably for syncing in snapshots we don't know about yet.
	// private applyBackupResultUpdates(BackupResult backupResult, Backup backup, Map updates) {
	// 	log.info("RESULT STATUS: ${updates.result}")
	// 	def status = updates.result ? RubrikBackupStatusUtility.getBackupStatus(updates.result) : MorpheusBackupStatusUtility.IN_PROGRESS
	// 	long sizeInMb = (updates.totalSize ?: 0) / 1048576
	// 	updates.backupSetId = updates.backupSetId ?: MoprheusBackupResultUtility.createBackupResultSetId()
	//
	// 	if(updates.error) {
	// 		backupResult.status = RubrikBackupStatusUtility.STATUS_FAILED
	// 		backupResult.errorOutput = updates.error
	// 	} else {
	// 		backupResult.externalId = updates.externalId
	// 		backupResult.status = status
	// 		backupResult.sizeInMb = sizeInMb
	// 	}
	//
	// 	// this might only be needed for syncing in new backup results
	// 	// config: [
	// 	// 	accountId:backup.account.id,
	// 	// 	backupId:backup.id,
	// 	// 	backupName:backup.name,
	// 	// 	backupType:backup.backupType.code,
	// 	// 	serverId:backup.serverId,
	// 	// 	active:true,
	// 	// 	containerId:backup.containerId,
	// 	// 	instanceId:backup.instanceId,
	// 	// 	containerTypeId:backup.containerTypeId,
	// 	// 	restoreType:backup.backupType.restoreType,
	// 	// 	startDay:new Date().clearTime(),
	// 	// 	startDate:new Date(),
	// 	// 	backupSetId:updates.backupSetId,
	// 	// 	backupRequestId: updates.backupRequestId
	// 	// ]
	//
	// }

}
