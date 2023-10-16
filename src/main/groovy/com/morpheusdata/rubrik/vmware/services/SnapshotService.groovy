package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.backup.util.BackupStatusUtility
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.Backup as BackupModel
import com.morpheusdata.model.BackupResult as BackupResultModel
import com.morpheusdata.model.ComputeServer as ComputeServerModel
import com.morpheusdata.model.MorpheusModel
import com.morpheusdata.model.projection.BackupResultIdentityProjection
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.RubrikPlugin
import com.morpheusdata.rubrik.services.ApiService
import groovy.util.logging.Slf4j
import io.reactivex.Observable
import java.time.Instant
import java.time.temporal.ChronoUnit

@Slf4j
class SnapshotService {

	private RubrikPlugin plugin
	private ApiService apiService

	SnapshotService(RubrikPlugin plugin) {
		this.plugin = plugin
		this.apiService = new RubrikVmwareApiService()
	}

	def executeCache(BackupProviderModel backupProviderModel, Map authConfig) {
		log.debug("executeCache: ${backupProviderModel.id}")
		try {
			plugin.morpheus.async.backup.listIdentityProjections(backupProviderModel)
				.doOnError() { e -> log.error("executeCache, error loading backup identity projections: ${e.message}", e) }
				.buffer(50)
				.flatMap() {  providerBackupIdp ->
					plugin.morpheus.async.backup.listById(providerBackupIdp.collect { it.id })
				}
				.buffer(50)
				.flatMap(){ List<BackupModel> backups ->
					plugin.morpheus.async.computeServer.listById(backups.collect { it.computeServerId })
						.map { ComputeServerModel server ->
							return [backup:backups.find{it.computeServerId == server.id }, server: server]
						}
				}.flatMap() {  Map<String, MorpheusModel> backupServerDto ->
					ServiceResponse snapshotListResults = apiService.listSnapshotsForVirtualMachine(authConfig, backupServerDto.server.externalId)
					log.debug("snapshotLIstResults: ${snapshotListResults}")
					List<Map> snapshotList = []
					if(snapshotListResults.success) {
						snapshotList = snapshotListResults.data.snapshots
					} else {
						def errorMsg = "Failed to load snapshots for sync"
						if(snapshotListResults.msg) {
							errorMsg += ": ${snapshotListResults.msg}"
						}
						log.error(errorMsg)
					}

					Observable<BackupResultIdentityProjection> backupResultIdentityProjections = plugin.morpheus.async.backup.backupResult.listIdentityProjections(
						new DataQuery(backupProviderModel.account)
							.withFilters(
								new DataFilter('backup.id', '=', backupServerDto.backup.id),
								new DataFilter('status', '!=', BackupResultModel.Status.START_REQUESTED),
								new DataFilter('status', '!=', BackupResultModel.Status.INITIALIZING),
								new DataFilter('status', '!=', BackupResultModel.Status.CANCELLED)
							)
					)
					SyncTask<BackupResultIdentityProjection, Map, BackupResultModel> syncTask = new SyncTask(backupResultIdentityProjections, snapshotList)
					syncTask.addMatchFunction { BackupResultIdentityProjection localItem, Map remoteItem ->
						localItem.externalId == remoteItem.id
					}.onDelete { List<BackupResultIdentityProjection> deleteList ->
						List<BackupResultIdentityProjection> filteredDeleteList = plugin.morpheus.services.backup.backupResult.listIdentityProjections(
							new DataQuery(backupProviderModel.account)
								.withFilter('id', 'in', deleteList.collect { it.id })
								.withFilter('status', '!=', BackupResultModel.Status.START_REQUESTED)
								.withFilter('status', '!=', BackupResultModel.Status.INITIALIZING)
								.withFilter('status', '!=', BackupResultModel.Status.CANCELLED)
								.withFilter('status', '!=', BackupResultModel.Status.IN_PROGRESS)
						)
						removeUnmatchedItems(filteredDeleteList)
					}.onAdd { List<Map> createList ->
						addMissingItems(createList, backupServerDto.backup, backupProviderModel)
					}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<BackupResultIdentityProjection, Map>> updateItems ->
						loadObjectDetails(updateItems)
					}.onUpdate { List<SyncTask.UpdateItem<BackupResultModel, Map>> updateList ->
						updateMatchedItems(updateList)
					}

					return syncTask.observe()
				}
				.doOnError() { e -> log.error("executeCache, error loading backup details: ${e.message}", e) }
				.blockingSubscribe()

		} catch(e) {
			log.error "Error in execute : ${e}", e
		}
	}

	ServiceResponse executeCleanup(BackupProviderModel backupProviderModel, Map opts=[:]) {
		// morpheus internal delete will clean up backup results
		return ServiceResponse.success()
	}

	private loadObjectDetails(List<SyncTask.UpdateItemDto<BackupResultIdentityProjection, Map>> itemList) {
		Map<Long, SyncTask.UpdateItemDto<BackupResultIdentityProjection, Map>> updateItemMap = itemList.collectEntries { [(it.existingItem.id): it]}
		List<Long> existingItemIds = itemList.collect { it.existingItem.id }
		Observable<BackupResultModel> itemDetailsList = plugin.morpheus.async.backup.backupResult.listById(existingItemIds)
		return itemDetailsList.map { BackupResultModel backupResultModel ->
			SyncTask.UpdateItemDto<BackupResultIdentityProjection, Map> matchItem = updateItemMap[backupResultModel.id]
			return new SyncTask.UpdateItem<BackupResultModel, Map>(existingItem:backupResultModel, masterItem:matchItem.masterItem)
		}
	}

	private addMissingItems(List<Map> itemList, BackupModel backupModel, BackupProviderModel backupProviderModel) {

		def newItems = []
		for(Map remoteItem in itemList) {
			Date createdDate = DateUtility.parseDate(remoteItem.date)
			Date createdDay = createdDate ? Date.from(createdDate.toInstant().truncatedTo(ChronoUnit.DAYS)) : null
			def add = new BackupResultModel(
				status         : BackupStatusUtility.SUCCEEDED,
				externalId     : remoteItem.id,
				account        : backupProviderModel.account,
				backup         : backupModel,
				backupName     : backupModel.name,
				backupType     : backupModel.backupType,
				serverId       : backupModel.computeServerId,
				active         : true,
				containerId    : backupModel.containerId,
				instanceId     : backupModel.instanceId,
				containerTypeId: backupModel.containerTypeId,
				startDay       : createdDay,
				startDate      : createdDate,
				endDay         : createdDay,
				endDate        : createdDate,
				backupSetId    : backupModel.backupSetId ?: BackupResultUtility.generateBackupResultSetId()
			)
			add.setConfigMap(remoteItem)
			newItems << add
		}
		plugin.morpheus.backup.backupResult.create(newItems).blockingGet()
	}

	private updateMatchedItems(List<SyncTask.UpdateItem<BackupResultModel, Map>> itemList) {
		List<BackupResultModel> updateList = []
		for(SyncTask.UpdateItem<BackupResultModel, Map> updateMap in itemList) {
			BackupResultModel localItem = updateMap.existingItem
			Map remoteItem = updateMap.masterItem
			def doUpdate = false

			if(localItem.backupName != remoteItem.name) {
				localItem.backupName = remoteItem.name
				doUpdate = true
			}

			if(doUpdate) {
				updateList << localItem
			}

		}
		if(updateList.size() > 0) {
			plugin.morpheus.async.backup.backupResult.save(updateList).blockingGet()
		}
	}

	private removeUnmatchedItems(Collection itemList) {
		log.debug("removing backup results {}", itemList.collect { it.id })
		plugin.morpheus.async.backup.backupResult.remove(itemList).blockingGet()
	}

}
