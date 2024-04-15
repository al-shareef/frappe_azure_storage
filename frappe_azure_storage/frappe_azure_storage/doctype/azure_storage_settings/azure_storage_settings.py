# Copyright (c) 2022, Lovin Maxwell and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import os

import frappe
from frappe import _
from frappe.utils import cint
from frappe.model.document import Document
from azure.storage.blob import ContainerClient
from frappe.utils.data import now_datetime
from rq.timeouts import JobTimeoutException
from frappe.integrations.offsite_backup_utils import (
	generate_files_backup,
	get_latest_backup_file,
	send_email,
	validate_file_size,
)
from frappe.utils.background_jobs import enqueue
from frappe_azure_storage.utils import logger as azLogger, now_ms
import shutil

class AzureStorageSettings(Document):
	def validate(self):
		if not self.enabled:
			return

	@frappe.whitelist()
	def back_up_azure(self,retry_count=0):
		take_backups_azure(retry_count)

def todays_date() : return now_datetime().strftime("%Y%m%d")
def todays_date_path() : 
    _ = now_datetime()
    return f"{_.strftime('%Y')}/{_.strftime('%m-%B')}/{_.strftime('%d')}"


@frappe.whitelist()
def take_backup():
	"""Enqueue long job for taking backup to Azure"""
	enqueue(
		"frappe_azure_storage.frappe_azure_storage.doctype.azure_storage_settings.azure_storage_settings.take_backups_azure",
		queue="long",
		timeout=1500,
	)
	frappe.msgprint(_("Queued for backup. It may take a few minutes to an hour."))


def take_backups_daily():
	take_backups_if("Daily")


def take_backups_weekly():
	take_backups_if("Weekly")
	if cint(frappe.db.get_value("Azure Storage Settings", None, "enabled")):
		take_backups_azure(with_files = True)


def take_backups_monthly():
	take_backups_if("Monthly")

def take_backups_if(freq):
	if cint(frappe.db.get_value("Azure Storage Settings", None, "enabled")):
		if frappe.db.get_value("Azure Storage Settings", None, "frequency") == freq:
			take_backups_azure(with_files = False)

# def take_backups_if(freq):
# 	if cint(frappe.db.get_value("Azure Storage Settings", None, "enabled")):
# 		with_files = frappe.db.get_value("Azure Storage Settings", None, "file_frequency") == freq
# 		if frappe.db.get_value("Azure Storage Settings", None, "file_frequency") == freq:
# 			take_backups_azure(with_files = with_files)
# 		elif frappe.db.get_value("Azure Storage Settings", None, "frequency") == freq:
# 			take_backups_azure(with_files = with_files)


@frappe.whitelist()
def take_backups_azure(retry_count=0, with_files = False):
	try:
		validate_file_size()
		backup_to_azure(with_files = False)
		send_email(True, "Azure Storage", "Azure Storage Settings", "notify_email")
	except JobTimeoutException:
		if retry_count < 2:
			args = {"retry_count": retry_count + 1}
			enqueue(
				"frappe_azure_storage.frappe_azure_storage.doctype.azure_storage_settings.azure_storage_settings.take_backups_azure",
				queue="long",
				timeout=1500,
				**args
			)
		else:
			notify()
	except Exception:
		notify()


def take_ab_back_up(folder, conn):
	# from abrajbay.utils.backups import BackupGenerator
	# from frappe.utils import get_backups_path
	# odb = BackupGenerator(
	# 	frappe.conf.ab_db_name,
	# 	frappe.conf.ab_db_user,
	# 	frappe.conf.ab_db_password,
	# 	db_host=frappe.conf.ab_db_host,
	# 	db_type=frappe.conf.db_type,
	# 	db_port=frappe.conf.ab_db_port,
	# )
	# try:
	# 	odb.take_dump()
	# 	filename = os.path.join(get_backups_path(), os.path.basename(odb.backup_path_db))
	# 	upload_file_to_azure(filename, folder, conn)
	# except Exception as ex:
	# 	frappe.log_error()
	# 	print("take_ab_back_up: %s" % (ex))
	pass


def notify():
	error_message = frappe.get_traceback()
	send_email(False, "Azure Storage", "Azure Storage Settings", "notify_email", error_message)


def backup_to_azure(with_files = False):
	from frappe.utils import get_backups_path
	

	doc = frappe.get_single("Azure Storage Settings")
	container = doc.default_container
	backup_files = cint(doc.backup_files)

	conn = ContainerClient.from_connection_string(doc.endpoint_url, container_name=container)

	if frappe.flags.create_new_backup:
		from frappe.utils.backups import new_backup
		backup = new_backup(
			ignore_files=False,
			backup_path_db=None,
			backup_path_files=None,
			backup_path_private_files=None,
			force=True,
		)
		db_filename = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_db))
		site_config = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_conf))
		if backup_files and with_files:
			files_filename = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_files))
			private_files = os.path.join(
				get_backups_path(), os.path.basename(backup.backup_path_private_files)
			)
	else:
		if backup_files and with_files:
			db_filename, site_config, files_filename, private_files = get_latest_backup_file(
				with_files=backup_files
			)

			if not files_filename or not private_files:
				generate_files_backup()
				db_filename, site_config, files_filename, private_files = get_latest_backup_file(
					with_files=backup_files
				)

		else:
			db_filename, site_config = get_latest_backup_file()

	folder = os.path.basename(db_filename)[:15] + "/"
	# for adding datetime to folder name
	upload_file_to_azure(db_filename, folder, conn)
	upload_file_to_azure(site_config, folder, conn)

	if backup_files and with_files:
		if private_files:
			upload_file_to_azure(private_files, folder, conn)

		if files_filename:
			upload_file_to_azure(files_filename, folder, conn)

	take_ab_back_up(folder, conn)

def upload_file_to_azure(filename, folder, conn):
	# destpath = os.path.join(folder, os.path.basename(filename))
	site_name = frappe.local.site
	# dPath = f"/mnt/abrajbay-db-backup/{todays_date_path()}/"
	fPath = f"{site_name}/DbBackups/{todays_date_path()}/{os.path.basename(filename)}"
	
	# try:
	# 	if not os.path.exists(dPath):
	# 		os.makedirs(dPath, exist_ok=True)
  	# 	# Saving Local
	# 	shutil.copy(filename, dPath)

	# except Exception as e:
	# 	azLogger.error("Error copy: %s" % (e))

	try:
		# Instantiate a new BlobClient
		blob_client = conn.get_blob_client(fPath)
		# [START upload_a_blob]
		# Upload content to block blob
		with open(filename, "rb") as data:
			blob_client.upload_blob(data, blob_type="BlockBlob")


	except Exception as e:
		frappe.log_error()
		azLogger.error("Error uploading: %s" % (e))

  	