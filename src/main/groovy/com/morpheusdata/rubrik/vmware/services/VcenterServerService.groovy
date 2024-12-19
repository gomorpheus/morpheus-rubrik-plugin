package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.RubrikPlugin
import groovy.util.logging.Slf4j

@Slf4j
class VcenterServerService {

	private RubrikPlugin plugin
	private RubrikVmwareApiService apiService
	private MorpheusContext morpheusContext

	VcenterServerService(MorpheusContext morpheusContext) {
		this.apiService = new RubrikVmwareApiService(morpheusContext)
		this.morpheusContext = morpheusContext
	}

	VcenterServerService(RubrikPlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.apiService = new RubrikVmwareApiService(morpheusContext)
		this.morpheusContext = morpheusContext
	}

	def executeRefresh(BackupProvider backupProviderModel, Map authConfig) {
		log.debug("refreshVCenterServers: {}", backupProviderModel)
		try {
			ServiceResponse listResults = apiService.getPlatformApiService(backupProviderModel.getConfigProperty("platformType")).listVCenterServers(authConfig)
			if(listResults.success) {
				log.debug("refreshVCenterServers: listResults: ${listResults.data}")
				listResults.data?.vcenterServers.each { server ->
					String serverId = server.id
					apiService.getPlatformApiService(backupProviderModel.getConfigProperty("platformType")).refreshVcenterServer(authConfig, serverId)
				}
			}
		} catch(e) {
			log.error("refreshVCenterServers error: ${e}", e)
		}
	}
}
