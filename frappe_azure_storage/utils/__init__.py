
__version__ = '0.0.1'

import frappe
from frappe.utils.logger import set_log_level
import time

set_log_level("DEBUG")
logger = frappe.logger("abrajbay_az", allow_site=True, file_count=5)

def now_ms(): return int(round(time.time() * 1000))


