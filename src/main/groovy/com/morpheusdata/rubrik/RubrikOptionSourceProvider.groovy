package com.morpheusdata.rubrik

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ReferenceData
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
		return new ArrayList<String>(['rubrikSlaDomains', 'rubrikPlatformTypes'])
	}

	def rubrikSlaDomains(args) {
		args = args instanceof Object[] ? args.getAt(0) : args
		log.debug("plugin rubrikSlaDomains args: ${args}")
		def rtn = []
		def cloudId = args.cloudId ?: args.zoneId
		Cloud cloud
		BackupProvider backupProvider
		if(!cloudId && args.containerId) {
			cloud = morpheus.services.workload.find(new DataQuery().withFilter('id', args.containerId.toLong()))?.server?.cloud
		}
		if(!cloud && cloudId) {
			cloud = morpheus.services.cloud.get(Long.parseLong(cloudId))
			log.debug("cloud: $cloud")
			log.debug("cloud backupProvider: $cloud.backupProvider")
			log.debug("cloud backupProvider type: $cloud.backupProvider.type")
		}

		if(cloud && cloud.backupProvider) {
			backupProvider = morpheus.services.backupProvider.get(cloud.backupProvider.id)
		}
		log.debug("Plugin rubrikSlaDomains Backup provider: ${backupProvider}")
		log.debug("Plugin rubrikSlaDomains Backup provider type: ${backupProvider?.type}")
		if(backupProvider) {
			def category = "${backupProvider?.type?.code}.backup.slaDomain.${backupProvider?.id}"
			log.debug("SLA DOMAIN LOOK UP CATEGORY: ${category}")
			List<ReferenceData> results = morpheus.services.referenceData.list(new DataQuery().withFilter("category", category))
			log.debug("rubrikSlaDomains results: ${results}")
			if(results.size() > 0) {
				results.each { policy ->
					rtn << [name: policy.name, id: policy.id, value: policy.id]
				}
			} else {
				rtn << [name: "No SLA Domains found for Rubrik", id:'']
			}

			rtn << [name: "Don't Protect", id: 'UNPROTECTED', value: 'UNPROTECTED']
			rtn << [name: "Clear current SLA Domain (Inherit from Parent)", id: 'INHERIT', value: 'INHERIT']
		} else {
			rtn << [name: "No Rubrik backup provider found.", id:'']
		}
		return rtn
	}

	def rubrikPlatformTypes(args) {
		args = args instanceof Object[] ? args.getAt(0) : args

		def rtn = [
			[name: 'Rubrik CDM', value: '', isDefault: args.domain?.id ? true : false], // CDM is the default for existing integrations to handle null value is a CDM integration
			[name: 'Rubrik Security Cloud', value: 'RSC', isDefault: args.domain?.id ? false : true ] // RSC is the default for new integrations
		]
		return rtn
	}
}
