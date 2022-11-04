package com.morpheusdata.rubrik.vmware

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.vmware.services.SnapshotService
import com.morpheusdata.rubrik.vmware.services.RubrikVmwareApiService
import com.morpheusdata.rubrik.vmware.services.VcenterServerService
import groovy.util.logging.Slf4j

//
// Equivalent to morpheus BackupType
//
@Slf4j
class RubrikVmwareBackupProvider extends AbstractBackupProvider {

	RubrikVmwareApiService apiService

	SnapshotService slaSnapshotService

	VcenterServerService vcenterServerService

	BackupExecutionProvider executionProvider;
	BackupRestoreProvider restoreProvider;

	RubrikVmwareBackupProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
		apiService = new RubrikVmwareApiService()
	}

	@Override
	String getCode() {
		return "rubrikVmware"
	}

	@Override
	String getName() {
		return "Rubrik VMware Backup Provider"
	}

	@Override
	boolean isPlugin() {
		return super.isPlugin()
	}

	@Override
	String getContainerType() {
		return "single"
	}

	@Override
	Boolean getCopyToStore() {
		return false
	}

	@Override
	Boolean getDownloadEnabled() {
		return false
	}

	@Override
	Boolean getRestoreExistingEnabled() {
		return true
	}

	@Override
	Boolean getRestoreNewEnabled() {
		return true
	}

	@Override
	String getRestoreType() {
		return "online"
	}

	@Override
	String getRestoreNewMode() {
		return "TEMP_EXTRACT"
	}

	@Override
	Boolean getHasCopyToStore() {
		return false
	}

	@Override
	Collection<OptionType> getOptionTypes() {
		return new ArrayList<OptionType>()
	}

	@Override
	RubrikVmwareBackupExecutionProvider getExecutionProvider() {
		if(!this.executionProvider) {
			this.executionProvider = new RubrikVmwareBackupExecutionProvider(getPlugin())
		}
		return this.executionProvider
	}

	@Override
	RubrikVmwareBackupRestoreProvider getRestoreProvider() {
		if(!this.restoreProvider) {
			this.restoreProvider = new RubrikVmwareBackupRestoreProvider(getPlugin())
		}
		return this.restoreProvider
	}

	@Override
	ServiceResponse refresh(Map authConfig, BackupProviderModel backupProviderModel) {
		log.debug("Refreshing Rubrik Backup Provider \"{}\"", backupProviderModel.name)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			getSlaSnapshotService().executeSync(backupProviderModel, authConfig)
			// getVcenterServerService().executeRefresh(backupProviderModel, authConfig)
		} catch(Exception e) {
			log.error("error refreshing backup provider {}::{}: {}", plugin.name, this.name, e)
		}
		return rtn
	}

	@Override
	ServiceResponse clean(BackupProviderModel backupProviderModel, Map opts) {
		ServiceResponse rtn = ServiceResponse.create([success: true, data:backupProviderModel])
		Boolean keepGoing = true

		def slaSnapshotCleanupResults = getSlaSnapshotService().executeCleanup(backupProviderModel, opts)
		if(!slaSnapshotCleanupResults.success) {
			keepGoing = false
			rtn.success = false
			rtn.msg = slaSnapshotCleanupResults.msg
		}

		return rtn
	}

	private SnapshotService getSlaSnapshotService() {
		if(!this.slaSnapshotService) {
			this.slaSnapshotService = new SnapshotService(getPlugin())
		}

		return this.slaSnapshotService
	}

	private VcenterServerService getVcenterServerService() {
		if(!this.vcenterServerService) {
		 	this.vcenterServerService = new VcenterServerService(getPlugin())
		}

		return this.vcenterServerService
	}

}
