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
import io.reactivex.Observable

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
			def slaDomainResults = apiService.listSlaDomains(authConfig)
			if(slaDomainResults.success && slaDomainResults.data?.size() > 0) {
				List<Map> slaDomainList = slaDomainResults.data.slaDomains
				Observable<ReferenceDataSyncProjection> referenceDataIdentityProjections = plugin.morpheus.referenceData.listByAccountIdAndCategory(backupProviderModel.account.id, objectCategory)
				SyncTask<ReferenceDataSyncProjection, Map, ReferenceDataModel> syncTask = new SyncTask(referenceDataIdentityProjections, slaDomainList)
				syncTask.addMatchFunction { ReferenceDataSyncProjection localItem, Map remoteItem ->
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
			List<ReferenceDataSyncProjection> referenceDataList = plugin.morpheus.referenceData.listByAccountIdAndCategory(backupProviderModel.account.id, objCategory).blockingSubscribe()
			plugin.morpheus.referenceData.remove(referenceDataList).blockingGet()
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
		Observable<ReferenceDataModel> itemDetailsList = plugin.morpheus.referenceData.listById(existingItemIds)
		return itemDetailsList.map { ReferenceDataModel referenceDataModel ->
			SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map> matchItem = updateItemMap[referenceDataModel.id]
			return new SyncTask.UpdateItem<ReferenceDataModel, Map>(existingItem:referenceDataModel, masterItem:matchItem.masterItem)
		}
	}

	private addMissingItems(List<Map> itemList, BackupProviderModel backupProviderModel) {
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
				externalId: remoteItem.id
			)
			add.setConfigMap(remoteItem)
			newItems << add
		}
		plugin.morpheus.referenceData.create(newItems).blockingGet()
	}

	private updateMatchedItems(List<SyncTask.UpdateItem<ReferenceDataModel, Map>> itemList) {
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
			}

		}
		if(updateList.size() > 0) {
			plugin.morpheus.referenceData.save(updateList).blockingGet()
		}
	}

	private removeUnmatchedItems(Collection itemList) {
		plugin.morpheus.referenceData.remove(itemList).blockingGet()
	}

	private getObjectCategory(BackupProviderModel backupProviderModel) {
		return "${backupProviderModel.type.code}.backup.slaDomain.${backupProviderModel.id}"
	}

}
