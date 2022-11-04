package com.morpheusdata.rubrik

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.model.BackupJob
import com.morpheusdata.response.ServiceResponse;

class RubrikBackupJobProvider implements BackupJobProvider {

	@Override
	ServiceResponse configureBackupJob(BackupJob backupJobModel, Map config, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse validateBackupJob(BackupJob backupJobModel, Map config, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse createBackupJob(BackupJob backupJobModel, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse cloneBackupJob(BackupJob sourceBackupJobModel, BackupJob backupJobModel, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse addToBackupJob(BackupJob backupJobModel, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse deleteBackupJob(BackupJob backupJobModel, Map opts) {
		return ServiceResponse.success();
	}

	@Override
	ServiceResponse executeBackupJob(BackupJob backupJob, Map opts) {
		return ServiceResponse.success();
	}

}
