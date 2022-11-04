package com.morpheusdata.rubrik

import com.morpheusdata.core.Plugin
import com.morpheusdata.rubrik.vmware.RubrikVmwareBackupProvider
import groovy.util.logging.Slf4j

@Slf4j
class RubrikPlugin extends Plugin {

	@Override
	String getCode() {
		return 'morpheus-rubrik-plugin'
	}

	@Override
	void initialize() {
		this.name = "Rubrik"
		RubrikBackupProvider backupProvider = new RubrikBackupProvider(this, morpheus)
		this.pluginProviders.put(backupProvider.code, backupProvider)

		// vmware
		RubrikVmwareBackupProvider vmwareBackupProvider = new RubrikVmwareBackupProvider(this, morpheus)
		this.pluginProviders.put(vmwareBackupProvider.code, vmwareBackupProvider)
		backupProvider.addScopedProvider(vmwareBackupProvider, "vmware", null)

		// hyperv
		// aws
		// nutanix

	}

	@Override
	void onDestroy() {
		// nothing
	}
}
