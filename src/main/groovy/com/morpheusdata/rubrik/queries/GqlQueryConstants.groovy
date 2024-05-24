package com.morpheusdata.rubrik.queries
import groovy.transform.CompileStatic

@CompileStatic
class GqlQueryConstants {
    static final listHosts = """
		query listHosts ($hostRoot:HostRoot!) {
			physicalHosts (hostRoot:$hostRoot) {
				nodes {
					id
					name
					effectiveSlaDomain {
						id
						name
					}
				}
			}
		}
	"""

    static final listSlaDomains = """
		query listSlaDomains {
		  slaDomains {
			count
			nodes {
			  id
			  name
			}
		  }
		}
	"""

}