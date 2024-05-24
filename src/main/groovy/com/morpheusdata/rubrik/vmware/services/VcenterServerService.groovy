package com.morpheusdata.rubrik.vmware.services


import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.RubrikPlugin
import groovy.util.logging.Slf4j

@Slf4j
class VcenterServerService {

	private RubrikPlugin plugin
	private RubrikVmwareApiService apiService

	VcenterServerService() {
		this.apiService = new RubrikVmwareApiService()
	}

	VcenterServerService(RubrikPlugin plugin) {
		this.plugin = plugin
		this.apiService = new RubrikVmwareApiService()
	}

	def executeRefresh(BackupProvider backupProviderModel, Map authConfig) {
		log.debug("refreshVCenterServers: {}", backupProviderModel)
		try {
			ServiceResponse listResults = apiService.getPlatformApiService(backupProviderModel.getConfigProperty("platformType")).listVCenterServers(authConfig)
			if(listResults.success) {
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
