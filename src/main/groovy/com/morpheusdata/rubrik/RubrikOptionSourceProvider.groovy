package com.morpheusdata.rubrik

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.rubrik.RubrikPlugin
import groovy.util.logging.Slf4j

@Slf4j
class RubrikOptionSourceProvider extends AbstractOptionSourceProvider {

	RubrikPlugin plugin
	MorpheusContext morpheusContext

	RubrikOptionSourceProvider(RubrikPlugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
	}

	@Override
	MorpheusContext getMorpheus() {
		return this.morpheusContext
	}

	@Override
	Plugin getPlugin() {
		return this.plugin
	}

	@Override
	String getCode() {
		return 'rubrik-option-source-plugin'
	}

	@Override
	String getName() {
		return 'Rubrik Option Source Plugin'
	}

	@Override
	List<String> getMethodNames() {
		return new ArrayList<String>(['rubrikSlaDomains'])
	}

	def rubrikSlaDomains(args) {
		def rtn = []
		def tmpAccount = args.currentUser.account
		def zone
		def backupProvider
		if(args.containerId && !args.zoneId) {
			zone = morpheus.contain
				Container.where{ account == tmpAccount && id == params.containerId.toLong() }.get()?.server?.zone
		}
		if(!zone && params.zoneId) {
			zone = morpheus.cloud.getCloudById(MorpheusUtils.parse )
				ComputeZone.read(params.zoneId)
		}
		if(zone && zone.backupProvider) {
			backupProvider = backupProviderService.loadBackupProvider(tmpAccount, zone.backupProvider.id)
		}
		if(!backupProvider) {
			backupProviderService.getBackupProvider(tmpAccount, 'rubrik')
		}
		if(backupProvider) {
			def results = ReferenceData.where { category == "${backupProvider?.type?.code}.backup.slaDomain.${backupProvider?.id}"}.list()
			if(results.size() > 0) {
				results.each { policy ->
					rtn << [name: policy.name, id: policy.id, value: policy.id]
				}
			} else {
				rtn << [name: "No SLA Domains found for Rubrik", id:'']
			}
		} else {
			rtn << [name: "No Rubrik backup provider found.", id:'']
		}
		return rtn
	}

}
