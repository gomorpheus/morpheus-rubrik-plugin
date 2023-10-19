package com.morpheusdata.rubrik.services

import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.ReferenceData as ReferenceDataModel
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.BackupProviderType as BackupProviderTypeModel
import com.morpheusdata.rubrik.RubrikPlugin
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class SlaDomainService {

	private RubrikPlugin plugin
	private ApiService apiService

	SlaDomainService(RubrikPlugin plugin) {
		this.plugin = plugin
		this.apiService = new ApiService()
	}

	def executeCache(BackupProviderModel backupProviderModel, Map authConfig) {
		log.debug("executeCache: ${backupProviderModel.id}")
		try {
			def objectCategory = getObjectCategory(backupProviderModel)
			log.debug("SLA DOMAIN SYNC CATEGORY: ${objectCategory}")
			def slaDomainResults = apiService.listSlaDomains(authConfig)
			log.debug("slaDomainResults: ${slaDomainResults}")
			if(slaDomainResults.success && slaDomainResults.data?.size() > 0) {
				List<Map> slaDomainList = slaDomainResults.data.slaDomains
				Observable<ReferenceDataSyncProjection> referenceDataIdentityProjections = plugin.morpheus.async.referenceData.listByAccountIdAndCategory(backupProviderModel.account.id, objectCategory)
				SyncTask<ReferenceDataSyncProjection, Map, ReferenceDataModel> syncTask = new SyncTask(referenceDataIdentityProjections, slaDomainList)
				syncTask.addMatchFunction { ReferenceDataSyncProjection localItem, Map remoteItem ->
					log.debug("MATCH: localItem.externalId == remoteItem.id: ${localItem.externalId == remoteItem.id}")
					return localItem.externalId == remoteItem.id
				}.onDelete { List<ReferenceDataSyncProjection> deleteList ->
					removeUnmatchedItems(deleteList)
				}.onAdd { List<Map> createList ->
					addMissingItems(createList, backupProviderModel)
				}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItems ->
					loadObjectDetails(updateItems)
				}.onUpdate { List<SyncTask.UpdateItem<ReferenceDataModel, Map>> updateList ->
					updateMatchedItems(updateList)
				}.start()
			}
		} catch(e) {
			log.error "Error in execute : ${e}", e
		}
	}

	def executeCleanup(BackupProviderModel backupProviderModel, Map opts=[:]) {
		def rtn = [success: true]
		try {
			def objCategory = getObjectCategory(backupProviderModel)
			List<ReferenceDataSyncProjection> referenceDataList = plugin.morpheus.async.referenceData.listByAccountIdAndCategory(backupProviderModel.account.id, objCategory).blockingSubscribe()
			plugin.morpheus.async.referenceData.bulkRemove(referenceDataList).blockingGet()
		} catch (Exception e) {
			log.error("Error removing SLA Domains for backup provider {}[{}]: {}", backupProviderModel.name, backupProviderModel.id, e)
			rtn.msg = "Error removing SLA Domains: ${e}"
			rtn.success = false
		}
		return rtn
	}

	private loadObjectDetails(List<SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> itemList) {
		Map<Long, SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItemMap = itemList.collectEntries { [(it.existingItem.id): it]}
		List<Long> existingItemIds = itemList.collect { it.existingItem.id }
		Observable<ReferenceDataModel> itemDetailsList = plugin.morpheus.async.referenceData.listById(existingItemIds)
		return itemDetailsList.map { ReferenceDataModel referenceDataModel ->
			SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map> matchItem = updateItemMap[referenceDataModel.id]
			return new SyncTask.UpdateItem<ReferenceDataModel, Map>(existingItem:referenceDataModel, masterItem:matchItem.masterItem)
		}
	}

	private addMissingItems(List<Map> itemList, BackupProviderModel backupProviderModel) {
		log.debug("addMissingItems: ${itemList}")
		String objectCategory = getObjectCategory(backupProviderModel)
		def newItems = []
		for(Map remoteItem in itemList) {
			def add = new ReferenceDataModel(
				account: backupProviderModel.account,
				code: "${objectCategory}.${remoteItem.id}",
				category: objectCategory,
				name: remoteItem.name,
				keyValue: remoteItem.id,
				value: remoteItem.id,
				externalId: remoteItem.id,
				type: 'string'
			)
			add.setConfigMap(remoteItem)
			newItems << add
			log.debug("Add SLA domain: ${add}")
		}
		plugin.morpheus.async.referenceData.bulkCreate(newItems).blockingGet()
	}

	private updateMatchedItems(List<SyncTask.UpdateItem<ReferenceDataModel, Map>> itemList) {
		log.debug("updateMatchedItems: ${itemList}")
		List<ReferenceDataModel> updateList = []
		for(SyncTask.UpdateItem<ReferenceDataModel, Map> updateMap in itemList) {
			ReferenceDataModel localItem = updateMap.existingItem
			Map remoteItem = updateMap.masterItem
			def doUpdate = false

			if(localItem.name != remoteItem.name) {
				localItem.name = remoteItem.name
				doUpdate = true
			}

			if(doUpdate) {
				updateList << localItem
				log.debug("Update SLA domain: ${localItem}")
			}

		}
		if(updateList.size() > 0) {
			plugin.morpheus.async.referenceData.bulkSave(updateList).blockingGet()
		}
	}

	private removeUnmatchedItems(Collection itemList) {
		log.debug("Remove unmatched Items: ${itemList}")
		plugin.morpheus.async.referenceData.bulkRemove(itemList).blockingGet()
	}

	private getObjectCategory(BackupProviderModel backupProviderModel) {
		return "${backupProviderModel.type.code}.backup.slaDomain.${backupProviderModel.id}"
	}

}
