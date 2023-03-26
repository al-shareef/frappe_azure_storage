// Copyright (c) 2022, Lovin Maxwell and contributors
// For license information, please see license.txt

frappe.ui.form.on('Azure Storage Settings', {
	refresh: function(frm) {
		frm.add_custom_button(__('Take Backup Now'), function(){
			frm.dashboard.set_headline_alert("Azure Backup Started!");
			frappe.call({
				// method: 'back_up_azure',
				// doc: frm.doc,
				method: 'frappe_azure_storage.frappe_azure_storage.doctype.azure_storage_settings.azure_storage_settings.take_backups_azure',
				// freeze: true,
				// freeze_message: __("Testing..."),
				callback: function(r) {
					if(!r.exc) {
						frappe.msgprint(__("Azure Backup complete!"));
						frm.dashboard.clear_headline();
					}
				}
			});
		});
	}
});
