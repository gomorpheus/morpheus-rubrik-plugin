## Rubrik

This is the official Morpheus plugin for interacting with the Rubrik Data Protection API. This plugin provides the automation of inserting provisioned Morpheus instances into Rubrik SLA Domains and the recovery of instances from Rubrik SLA Domain snaphots.

### Building

This is a Morpheus plugin that leverages the `morpheus-plugin-core` which can be referenced by visiting [https://developer.morpheusdata.com](https://developer.morpheusdata.com). It is a groovy plugin designed to be uploaded into a Morpheus environment via the `Administration -> Integrations -> Plugins` section. To build this product from scratch simply run the shadowJar gradle task on java 11:

```bash
./gradlew shadowJar
```

A jar will be produced in the `build/lib` folder that can be uploaded into a Morpheus environment.


### Configuring

Once the plugin is loaded in the environment. Rubrik Becomes available in `Backups -> Integrations`.

When adding the integration simply enter the URL of the Rubrik host and the desired user's API token.

