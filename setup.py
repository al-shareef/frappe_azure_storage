from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in frappe_azure_storage/__init__.py
from frappe_azure_storage import __version__ as version

setup(
	name="frappe_azure_storage",
	version=version,
	description="Azure Storage for Frappe",
	author="Lovin Maxwell",
	author_email="lovinmaxwell@gmail.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
