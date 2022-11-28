package com.morpheusdata.rubrik

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.core.backup.BackupProvider
import com.morpheusdata.core.backup.BackupTypeProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.BackupProviderType
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.rubrik.services.ApiService
import com.morpheusdata.rubrik.services.SlaDomainService
import com.morpheusdata.rubrik.vmware.RubrikVmwareBackupProvider
import groovy.util.logging.Slf4j
import groovy.json.JsonOutput
import com.morpheusdata.core.util.ConnectionUtils

//
// Equivalent to Morpheus BackupProviderType
//

@Slf4j
class RubrikBackupProvider extends AbstractBackupProvider {

	static String LOCK_NAME = 'backups.rubrik'

	ApiService apiService

	SlaDomainService SlaDomainService

	BackupJobProvider backupJobProvider;

	RubrikBackupProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
		apiService = new ApiService()

		// vmware
		RubrikVmwareBackupProvider vmwareBackupProvider = new RubrikVmwareBackupProvider(plugin, morpheus)
		plugin.pluginProviders.put(vmwareBackupProvider.code, vmwareBackupProvider)
		addScopedProvider(vmwareBackupProvider, "vmware", null)
		// hyperv
		// aws
		// nutanix
	}

	@Override
	String getCode() {
		return 'rubrik'
	}

	@Override
	String getName() {
		return 'Rubrik'
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"rubrik.svg", darkPath: "rubrik-dark.svg")
	}

	// TODO: remove this in favor of option types on the backup type?
	@Override
	public String getViewSet() {
		'rubrik'
	}

	@Override
	public Boolean getEnabled() { return true; }

	@Override
	public Boolean getCreatable() { return true; }

	@Override
	public Boolean getRestoreNewEnabled() { return true; }

	@Override
	public Boolean getHasBackups() { return true; }

	@Override
	public Boolean getHasCreateJob() { return true; }

	@Override
	public Boolean getHasCloneJob() { return true; }

	@Override
	public Boolean getHasAddToJob() { return true; }

	@Override
	public Boolean getHasOptionalJob() { return true; }

	@Override
	public Boolean getHasSchedule() { return true; }

	@Override
	public String getDefaultJobType() { return "none"; }

	@Override
	public Boolean getHasRetentionCount() { return true; }

	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList();
		optionTypes << new OptionType(
			code:"backupProviderType.${this.getCode()}.host", inputType:OptionType.InputType.TEXT, name:'host', category:"backupProviderType.${this.getCode()}",
			fieldName:'host', fieldCode: 'gomorpheus.optiontype.Host', fieldLabel:'Host', fieldContext:'domain', fieldGroup:'default',
			required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
			displayOrder:10, fieldClass:null
		)
		optionTypes << new OptionType(
			code:"backupProviderType.${this.getCode()}.credential", inputType:OptionType.InputType.CREDENTIAL, name:'credentials', category:"backupProviderType.${this.getCode()}",
			fieldName:'type', fieldCode:'gomorpheus.label.credentials', fieldLabel:'Credentials', fieldContext:'credential', optionSource:'credentials',
			required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:'local', custom:false,
			displayOrder:25, fieldClass:null, wrapperClass:null, config: JsonOutput.toJson([credentialTypes:['api-key']]).toString()
		)
		optionTypes << new OptionType(
			code:"backupProviderType.${this.getCode()}.serviceToken", inputType:OptionType.InputType.PASSWORD, name:'password', category:"backupProviderType.${this.getCode()}",
			fieldName:'serviceToken', fieldCode: 'gomorpheus.optiontype.ApiToken', fieldLabel:'API Token', fieldContext:'domain', fieldGroup:'default',
			required:false, enabled:true, requireOnCode:'credential.type:local', editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
			displayOrder:30, fieldClass:null, localCredential:true
		)

		return optionTypes
	}

	@Override
	Collection<OptionType> getReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	@Override
	Collection<OptionType> getReplicationOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	@Override
	Collection<OptionType> getBackupJobOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	@Override
	Collection<OptionType> getBackupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()

		optionTypes << new OptionType(
			code:'backup.rubrik.slaDomain', inputType:OptionType.InputType.SELECT, name:'rubrikSlaDomain', optionSource:'rubrikSlaDomains',
			category:'backup.rubrik', fieldName:'rubrikSlaDomain', fieldCode: 'gomorpheus.label.slaDomain', fieldLabel:'SLA Domain', fieldContext:'backup.config',
			required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
			displayOrder:0, fieldClass:null
		)

		return optionTypes;
	}

	@Override
	Collection<OptionType> getInstanceReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}


	@Override
	BackupJobProvider getBackupJobProvider() {
		if(!this.backupJobProvider) {
			this.backupJobProvider = new DefaultBackupJobProvider(getPlugin(), morpheus);
		}
		return this.backupJobProvider
	}

	SlaDomainService getSlaDomainService() {
		if(!this.SlaDomainService) {
			this.SlaDomainService = new SlaDomainService(getPlugin())
		}

		return this.SlaDomainService
	}

	// provider
	@Override
	ServiceResponse configureBackupProvider(com.morpheusdata.model.BackupProvider backupProviderModel, Map config, Map opts) {
		backupProviderModel.host = opts.provider.host
		if(backupProviderModel.host) {
			backupProviderModel.serviceUrl = normalizeApiUrl(backupProviderModel.getHost())
		}

		return ServiceResponse.success(backupProviderModel)
	}
	@Override
	ServiceResponse validateBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		def rtn = [success:false, errors:[:]]
		try {
			def apiOpts = [:]

			if(opts.provider?.size() < 1) {
				rtn.msg = 'Enter a host'
				rtn.errors.host = 'Enter a host'
			}

			def localCredentials = (backupProviderModel.credentialData?.type ?: 'local') == 'local'
			if((localCredentials && !backupProviderModel?.serviceToken) || (!localCredentials && !backupProviderModel.credentialData?.password)) {
				rtn.msg = rtn.msg ?: 'Enter an api token'
				rtn.errors.serviceToken = 'Enter an api token'
			}

			if(rtn.errors.size() == 0) {
				def testResults = verifyAuthentication(backupProviderModel, apiOpts)
				log.debug("api test results: {}", testResults)
				if(testResults.success == true) {
					rtn.success = true
				} else if(testResults.invalidLogin == true) {
					rtn.msg = testResults.msg ?: 'unauthorized - invalid credentials'
				} else if(testResults.found == false) {
					rtn.msg = testResults.msg ?: 'Rubrik service not found - invalid host'
				} else {
					rtn.msg = testResults.msg ?: 'unable to connect to Rubrik service'
				}
			}
		} catch(e) {
			log.error("error validating Rubrik configuration: ${e}", e)
			rtn.msg = 'unknown error connecting to Rubrik'
			rtn.success = false
		}
		if(rtn.success) {
			return ServiceResponse.success(backupProviderModel, rtn.msg)
		} else {
			return ServiceResponse.error(rtn.msg, rtn.errors as Map, backupProviderModel)
		}
	}

	@Override
	ServiceResponse deleteBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		ServiceResponse rtn = ServiceResponse.create([success: true, data:backupProviderModel])
		Boolean keepGoing = true

		def slaCleanupResults = getSlaDomainService().executeCleanup(backupProviderModel, opts)
		if(!slaCleanupResults.success) {
			keepGoing = false
			rtn.success = false
			rtn.msg = slaCleanupResults.msg
		}

		if(keepGoing) {
			List<BackupTypeProvider> subProviders = getScopedProviders().collect { it.backupTypeProvider }
			for(BackupTypeProvider subProvider in subProviders) {
				if(keepGoing) {
					ServiceResponse subProviderResults = subProvider.clean(backupProviderModel, opts)
					if(subProviderResults.success == false) {
						keepGoing = false
						rtn.success = false
						rtn.msg = subProviderResults.msg
					}

				}
			}
		}

		return rtn
	}

	@Override
	ServiceResponse refresh(BackupProviderModel backupProviderModel) {
		ServiceResponse rtn = ServiceResponse.prepare()
		log.debug("refresh backup provider: [{}:{}]", backupProviderModel.name, backupProviderModel.id)
		try {
			def authConfig = apiService.getAuthConfig(backupProviderModel)
			def apiOpts = [authConfig:authConfig]
			def apiUrl = authConfig.apiUrl
			def apiUri = new URI(apiUrl)
			def apiHost = apiUri.getHost()
			def apiPort = apiUri.getPort() ?: apiUrl?.startsWith('https') ? 443 : 80
			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)
			log.info("backup provider host online: {}", hostOnline)
			if(hostOnline) {
				backupProviderModel.type = loadFullBackupProviderType(backupProviderModel)
				def testResults = verifyAuthentication(backupProviderModel, apiOpts)
				if(testResults.success == true) {
					morpheus.backupProvider.updateStatus(backupProviderModel, 'ok', null).subscribe().dispose()
					getSlaDomainService().executeCache(backupProviderModel, authConfig)
					// Execute refresh on sub providers (vmware, aws, hyperv, etc)
					// each provider has unique API endpoints for things like snapshots
					List<BackupTypeProvider> subProviders = getScopedProviders().collect { it.backupTypeProvider }
					for(BackupTypeProvider subProvider in subProviders) {
						subProvider.refresh(authConfig, backupProviderModel)
					}
				} else {
					if(testResults.invalidLogin == true) {
						morpheus.backupProvider.updateStatus(backupProviderModel, 'error', 'invalid credentials').subscribe().dispose()
					} else {
						morpheus.backupProvider.updateStatus(backupProviderModel, 'error', 'error connecting').subscribe().dispose()
					}
				}
			} else {
				morpheus.backupProvider.updateStatus(backupProviderModel, 'offline', 'Rubrik service not reachable').subscribe().dispose()
			}
			rtn.success = true
		} catch(e) {
			log.error("refreshBackupProvider error: ${e}", e)
		}

		return rtn
	}

	private verifyAuthentication(BackupProviderModel backupProviderModel, Map opts) {
		def rtn = [success:false, invalidLogin:false, found:true]
		opts.authConfig = opts.authConfig ?: apiService.getAuthConfig(backupProviderModel)
		def requestResults = apiService.listHosts(opts.authConfig)
		if(requestResults.success == true) {
			rtn.success = true
		} else {
			if(requestResults?.errorCode == '404' || requestResults?.errorCode == 404)
				rtn.found = false
			if(requestResults?.errorCode == '401' || requestResults?.errorCode == 401)
				rtn.invalidLogin = true
		}
		return rtn
	}

	private String normalizeApiUrl(String host) {
		def scheme = host.contains("http") ? "" : "https://"
		def apiUrl = "${scheme}${host}"

		return apiUrl
	}

	private loadFullBackupProviderType(BackupProviderModel backupProviderModel) {
		BackupProviderType backupProviderTypeModel = plugin.morpheus.backupProvider.type.getById(backupProviderModel.type.id).blockingGet()
		return backupProviderTypeModel ?: backupProviderModel.type
	}
}
