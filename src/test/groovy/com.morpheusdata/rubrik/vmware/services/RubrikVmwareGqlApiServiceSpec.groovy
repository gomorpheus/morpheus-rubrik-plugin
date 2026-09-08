package com.morpheusdata.rubrik.vmware.services

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.response.ServiceResponse
import spock.lang.Specification
import spock.lang.Subject

class RubrikVmwareGqlApiServiceSpec extends Specification {

	@Subject
	RubrikVmwareGqlApiService service

	def setup() {
		MorpheusContext context = Mock(MorpheusContext)
		service = new RubrikVmwareGqlApiService(context)
	}

	void "findMatchedCluster returns null instead of throwing NPE when physicalPath has no VSphereHost"() {
		given:
		def vmIdResponse = ServiceResponse.success([
			vSphereVmNewConnection: [
				[physicalPath: [[objectType: 'VSphereDatacenter']]]
			]
		])

		when:
		def result = service.findMatchedCluster([:], vmIdResponse, 'host-123')

		then:
		noExceptionThrown()
		result == null
	}

	void "findMatchedCluster returns null when the response was not successful"() {
		given:
		def vmIdResponse = ServiceResponse.error('api error')

		when:
		def result = service.findMatchedCluster([:], vmIdResponse, 'host-123')

		then:
		noExceptionThrown()
		result == null
	}

	void "findMatchedCluster returns null when vSphereVmNewConnection is missing"() {
		given:
		def vmIdResponse = ServiceResponse.success([:])

		when:
		def result = service.findMatchedCluster([:], vmIdResponse, 'host-123')

		then:
		noExceptionThrown()
		result == null
	}

	void "findMatchedCluster matches the node whose host id matches the parent server external id"() {
		given:
		def matchingNode = [physicalPath: [[objectType: 'VSphereHost', fid: 'host-fid-1']]]
		def vmIdResponse = ServiceResponse.success([
			vSphereVmNewConnection: [matchingNode]
		])
		def gqlService = Spy(RubrikVmwareGqlApiService, constructorArgs: [Mock(MorpheusContext)])
		gqlService.getHostById(_, 'host-fid-1') >> ServiceResponse.success([vSphereHost: [cdmId: ['host-42']]])

		when:
		def result = gqlService.findMatchedCluster([:], vmIdResponse, 'vm-42')

		then:
		result == matchingNode
	}
}
