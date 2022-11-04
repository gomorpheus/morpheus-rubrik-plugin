package com.morpheusdata.rubrik.util

import com.morpheusdata.core.backup.util.BackupStatusUtility as MorpheusBackupStatusUtility
import com.morpheusdata.model.BackupResult

class RubrikBackupStatusUtility {
	public static String STATUS_QUEUED = "QUEUED"
	public static String STATUS_ACQUIRING = "ACQUIRING"
	public static String STATUS_RUNNING = "RUNNING"
	public static String STATUS_FINISHING = "FINISHING"
	public static String STATUS_CANCEL_REQUESTED = "CANCEL REQUESTED"
	public static String STATUS_CANCELED = "CANCELED"
	public static String STATUS_SUCCEEDED = "SUCCEEDED"
	public static String STATUS_FAILED = "FAILED"

	static getBackupStatus(backupState) {
		def status
		if(backupState == STATUS_FAILED ) {
			status = MorpheusBackupStatusUtility.FAILED
		} else if(backupState == STATUS_CANCELED) {
			status = MorpheusBackupStatusUtility.CANCELLED
		} else if([STATUS_RUNNING, STATUS_FINISHING].contains(backupState)) {
			status = MorpheusBackupStatusUtility.IN_PROGRESS
		} else if(backupState == STATUS_SUCCEEDED) {
			status = MorpheusBackupStatusUtility.SUCCEEDED
		} else if([STATUS_ACQUIRING, STATUS_QUEUED].contains(backupState)) {
			status = MorpheusBackupStatusUtility.START_REQUESTED
		} else if(backupState == STATUS_CANCEL_REQUESTED) {
			status = MorpheusBackupStatusUtility.CANCEL_REQUESTED
		}

		return status
	}
}
