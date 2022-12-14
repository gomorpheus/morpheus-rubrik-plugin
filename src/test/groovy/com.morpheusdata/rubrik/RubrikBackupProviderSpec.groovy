package com.morpheusdata.rubrik


import com.morpheusdata.core.Plugin
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.rubrik.services.ApiService
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

class RubrikBackupProviderSpec extends Specification {

	@Subject@Shared
	RubrikBackupProvider provider
	@Shared
	ApiService apiService
	@Shared
	MorpheusContext context


	def setup() {
		Plugin plugin = Mock(Plugin)
		MorpheusContext context = Mock(MorpheusContext)
		apiService = Mock(ApiService)
		provider = new RubrikBackupProvider(plugin, context, apiService)
	}

	void "Provider valid"() {
		expect:
		provider
	}


	void "validate - fail"() {
		given:
		Cloud cloud = new Cloud(configMap: [doApiKey: 'abc123', doUsername: 'user'])

		when:
		def res = provider.validate(cloud)

		then:
		!res.success
		res.msg == 'Choose a datacenter'
	}

	void "validate"() {
		// given:
		// Cloud cloud = new Cloud(configMap: [doApiKey: 'abc123', doUsername: 'user', datacenter: 'nyc1'])
		//
		// when:
		// def res = provider.validate(cloud)
		//
		// then:
		// res.success
	}

}
