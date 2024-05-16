package com.morpheusdata.rubrik.vmware.services


import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.RubrikPlugin
import groovy.util.logging.Slf4j

@Slf4j
class VcenterServerService {

	private RubrikPlugin plugin
	private RubrikVmwareApiRestService apiRestService
	private RubrikVmwareApiGqlService apiGqlService

	VcenterServerService() {
		this.apiRestService = new RubrikVmwareApiRestService()
		this.apiGqlService = new RubrikVmwareApiGqlService()
	}

	VcenterServerService(RubrikPlugin plugin) {
		this.plugin = plugin
		this.apiRestService = new RubrikVmwareApiRestService()
		this.apiGqlService = new RubrikVmwareApiGqlService()
	}

	def executeRefresh(BackupProvider backupProviderModel, Map authConfig) {
		log.debug("refreshVCenterServers: {}", backupProviderModel)
		try {
			if(backupProviderModel.platform == "rsc") {
				ServiceResponse listResults = apiGqlService.listVCenterServers(authConfig)
				if(listResults.success) {
					listResults.data?.vcenterServers.each { server ->
						String serverId = server.id
						apiGqlService.refreshVcenterServer(authConfig, serverId)
					}
				}
			} else {
				ServiceResponse listResults = apiRestService.listVCenterServers(authConfig)
				if (listResults.success) {
					listResults.data?.vcenterServers.each { server ->
						String serverId = server.id
						apiRestService.refreshVcenterServer(authConfig, serverId)
					}
				}
			}
		} catch(e) {
			log.error("refreshVCenterServers error: ${e}", e)
		}
	}
}
