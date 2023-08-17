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
		log.debug("configuration opts.config: {}", opts.config)
		backup.setConfigProperty('backupConsistency', opts.config?.backupConsistency)
		backup.setConfigProperty('backupPolicy', opts.config?.backupPolicy)

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
			BackupProvider backupProvider = backup.backupProvider
			log.debug("backup config: ${backup.getConfigMap()}")
			rtn.success = true
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
					rtn.success = true
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
				log.debug("backup config: ${backup.getConfigMap()}")
				rtn.success = true
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
			rtn.data.backupResult.status = "succeeded"
			rtn.data.updates = true
			rtn.success = true
			log.debug("refreshing backup result {} completed with {}", backupResult.id, rtn.data.backupResult.status)
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
