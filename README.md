# Morpheus Rubrik Plugin

This plugin provides backup integration between [Rubrik](https://www.rubrik.com) and [Morpheus](https://morpheusdata.com). It enables SLA Domain sync, VMware VM backup protection via Rubrik CDM or Rubrik Security Cloud (RSC), snapshot sync, and VM restore workflows from within the Morpheus platform.

## 📑 Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Repository structure](#repository-structure)
- [Building the plugin](#building-the-plugin)
- [License](#license)
- [Installing](#installing)
- [Detailed Usage Step](#detailed-usage-step)
- [API Endpoints](#api-endpoints)

---

## Features

### Backup Integration

The plugin registers a Rubrik `BackupProvider` that connects Morpheus backup workflows to Rubrik. Supported integration behavior includes:

- Support for both Rubrik CDM (token-based) and Rubrik Security Cloud/RSC (client ID/secret-based) platforms
- Validate connectivity and credentials against the configured Rubrik host
- Add VMware instances to existing Rubrik SLA Domains
- Track provider health during refresh, including host connectivity checks

### Rubrik Sync

The following Rubrik resources are discovered and kept in sync:

- **SLA Domains** — Rubrik SLA Domains represented as Morpheus backup jobs, assignable to protected instances
- **VMware Hosts** — vCenter hosts registered with Rubrik
- **VMware Virtual Machines** — VMware VMs registered with Rubrik, matched to Morpheus instances
- **Snapshots** — VM snapshots represented as Morpheus backup results

### Backup and Restore Operations

VMware VM protection and restore workflows are available through the Morpheus backup framework. Supported operations include:

- Assign a Morpheus instance to a Rubrik SLA Domain to enable backup protection
- Trigger on-demand snapshots (backups) of a protected VM
- Restore a snapshot to the original VM location (instant recovery)
- Restore a snapshot to a new VM location using a selected datastore and host
- Clean up SLA Domain assignments and sub-provider resources when the backup provider is removed

---

## Requirements

| Component | Minimum Version |
|-----------|----------------|
| Morpheus | 8.0.11 |

---

## Repository structure

- `src/main/groovy` - Source code for the plugin
    - `RubrikPlugin.groovy` - Plugin entry point; registers the backup provider and option source provider
    - `RubrikBackupProvider.groovy` - Top-level Rubrik `BackupProvider`; handles provider configuration, validation, refresh, and SLA Domain option types
    - `RubrikOptionSourceProvider.groovy` - Supplies option lists (e.g. platform types, SLA Domains) for Morpheus UI forms
    - `services/ApiService.groovy`, `services/RestApiService.groovy`, `services/GqlApiService.groovy` - Base API clients for Rubrik CDM (REST) and RSC (GraphQL)
    - `services/PlatformApiServiceInterface.groovy` - Common interface implemented by both the REST and GraphQL API services
    - `services/SlaDomainService.groovy` - Caches and syncs Rubrik SLA Domains
    - `util/RubrikBackupStatusUtility.groovy` - Utility for mapping Rubrik snapshot/job status to Morpheus backup status
    - `queries/GqlQueryConstants.groovy` - GraphQL query/mutation strings used against Rubrik Security Cloud
    - `vmware/` - VMware-specific scoped backup provider
        - `RubrikVmwareBackupProvider.groovy` - Scoped `BackupProvider` for VMware workloads
        - `RubrikVmwareBackupExecutionProvider.groovy` - Executes on-demand VMware VM backups
        - `RubrikVmwareBackupRestoreProvider.groovy` - Restores VMware VM snapshots to existing or new VMs
        - `services/RubrikVmwareRestApiService.groovy`, `services/RubrikVmwareGqlApiService.groovy` - VMware host/VM/snapshot operations over REST (CDM) and GraphQL (RSC)
        - `services/VcenterServerService.groovy`, `services/SnapshotService.groovy` - Sync helpers for vCenter hosts and VM snapshots
        - `queries/RubrikVmwareGqlQueryConstants.groovy` - GraphQL queries specific to VMware VM/host/snapshot discovery
- `src/assets/images` - Plugin assets, including the Rubrik logo (`rubrik.svg`, `rubrik-dark.svg`) used in the Morpheus UI
- `build.gradle` and `gradle.properties` - Build configuration and dependency/version properties for the plugin

---

## Building the plugin

Run the following command to compile and package the plugin jar:

```bash
./gradlew shadowJar
```

The plugin JAR will be written to `build/libs/`.

To execute tests, use the following command:

```bash
./gradlew test
```

---

## License

Copyright 2023 Morpheus Data, LLC. Licensed under the [Apache License, Version 2.0](LICENSE).

---

## Installing

1. Download the latest `.jar` from the [Releases](https://github.com/HewlettPackard/morpheus-rubrik-plugin/releases) page, or [build it yourself](#building-the-plugin).
2. In Morpheus, navigate to **Administration → Integrations → Plugins**.
3. Click **Browse** and upload the `.jar` file.
4. The **Rubrik** backup integration will appear after the plugin loads.

---

## Detailed Usage Step

When adding a Rubrik integration in Morpheus (**Backups → Integrations → Add Backup Integration**), provide the following:

| Field | Description |
|-------|-------------|
| **Platform** | The Rubrik platform type: CDM or RSC (Rubrik Security Cloud) |
| **Host** | Hostname/URL of the Rubrik CDM cluster or RSC instance |
| **Credentials** | Select local credentials or a stored credential; CDM uses an API Key credential, RSC uses Client ID/Secret credentials |
| **API Key** | Rubrik CDM API token (required for local credentials on CDM) |
| **Client ID / Client Secret** | RSC service account client ID and secret (required for local credentials on RSC) |

Once the integration is added and validated, Morpheus will sync available SLA Domains, vCenter hosts, and VMware virtual machines from Rubrik.

**Protecting an Instance**

A backup can be configured either during the instance creation wizard (Backup step) or after provisioning (instance **Backups** tab → **Create Backup**). In both cases, provide the following:

| Field | Description                                                                                                |
|-------|------------------------------------------------------------------------------------------------------------|
| **Backup Type** | Select the Rubrik backup provider to protect the instance's VM                                             |
| **SLA Domain** | The Rubrik SLA Domain to assign the instance's VM to                                                       |
| **Backup Job Type** | Add the instance to an existing backup job, create a new job, clone a job, or run the backup without a job |
| **Schedule** | The backup schedule to associate with the job (when creating a new job)                                    |

Once configured, Morpheus assigns the instance's VM to the selected SLA Domain in Rubrik and runs backups according to the chosen job type and schedule.

**Executing a Backup**

In addition to scheduled runs, a backup can be executed on demand at any time from the instance's **Actions** dropdown by clicking **Create Snapshot**. This triggers Rubrik to take an on-demand snapshot of the instance's VM outside of its assigned schedule, and the resulting snapshot is tracked in Morpheus as a backup result.

**Restoring a Backup**
1. From the instance's backup results, select a snapshot to restore.
2. Restore in place to recover the original VM, or restore to a new VM by selecting a target datastore and host.

---

## API Endpoints

The plugin communicates with Rubrik using two platform-specific APIs, selected by the configured **Platform** type:

**Rubrik CDM (REST API, base path `/api/v1`)**
- `/session` (POST, DELETE) - Authenticate and terminate a session
- `/host` (GET) - List physical hosts registered with Rubrik
- `/sla_domain` (GET) - List available SLA Domains
- `/vmware/vcenter` (GET) - List vCenter servers registered with Rubrik
- `/vmware/vcenter/{serverId}/refresh` (POST) - Refresh a vCenter server's inventory in Rubrik
- `/vmware/host` (GET) - List vCenter hosts registered with Rubrik
- `/vmware/host/{hostId}` (GET) - Retrieve a specific vCenter host
- `/vmware/vm` (GET) - List and look up VMware virtual machines
- `/vmware/vm/{vmId}` (GET, PATCH) - Retrieve or update a virtual machine (e.g. assign to an SLA Domain)
- `/vmware/vm/{vmId}/snapshot` (GET, POST) - List snapshots for a VM or trigger an on-demand snapshot
- `/vmware/vm/snapshot/{snapshotId}` (GET, DELETE) - Retrieve or delete a specific snapshot
- `/vmware/vm/snapshot/{snapshotId}/instant_recover` (POST) - Restore a snapshot to its original VM location
- `/vmware/vm/snapshot/{snapshotId}/export` (POST) - Restore a snapshot to a new VM
- `/vmware/vm/snapshot/mount/{mountId}` (GET) - Retrieve mount status for a restore operation
- `/vmware/vm/request/{requestId}` (GET) - Poll the status of an async VM request (snapshot, restore, or refresh job)

**Rubrik Security Cloud (GraphQL API, `/api/graphql`)**
- `/client_token` (POST) - Obtain an access token using a Client ID/Secret
- `/api/graphql` (POST) - Executes all GraphQL queries and mutations against RSC, including:
    - `listHosts`, `getHost`, `getHostById` - Physical host discovery
    - `listSlaDomains` - SLA Domain discovery
    - `listVCenterServers`, `vSphereVCenterConnection`, `refreshVcenterServer` - vCenter server discovery and inventory refresh
    - `listVirtualMachines`, `getVirtualMachine`, `getVirtualMachineByMoid`, `updateVirtualMachine` - VMware VM discovery and SLA Domain assignment
    - `vSphereHost`, `vSphereHostConnection` - VMware host discovery
    - `backupVirtualMachine` (`vsphereOnDemandSnapshot`) - Trigger an on-demand snapshot
    - `listSnapshotsForVirtualMachine` (`snapshotsListConnection`), `getSnapshot`, `deleteSnapshot` - Snapshot listing, lookup, and deletion
    - `restoreSnapshotToVirtualMachine` (`vsphereVmInitiateInstantRecoveryV2`) - Restore a snapshot to its original VM location
    - `restoreSnapshotToNewVirtualMachine` (`vsphereVmExportSnapshotV3`) - Restore a snapshot to a new VM
    - `getMounts` (`vSphereMountConnection`) - Retrieve mount status for a restore operation
    - `getVmTaskRequest` (`vSphereVMAsyncRequestStatus`) - Poll the status of an async VM task
