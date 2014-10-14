import logging
import os
from subprocess import Popen, PIPE, STDOUT, list2cmdline
from holland.core.exceptions import BackupError
from holland.lib.compression import open_stream, lookup_compression

LOG = logging.getLogger(__name__)

# Specification for this plugin
# See: http://www.voidspace.org.uk/python/validate.html
CONFIGSPEC = """
[tar]
directory = string(default='/home')
""".splitlines()

class TarPlugin(object):
	def __init__(self, name, config, target_directory, dry_run=False):
		"""Create a new TarPlugin instance

		:param name: unique name of this backup
		:param config: dictionary config for this plugin
		:param target_directory: str path, under which backup data should be
		                         stored
		:param dry_run: boolean flag indicating whether this should be a real
		                backup run or whether this backup should only go
		                through the motions
		"""
		self.name = name
		self.config = config
		self.target_directory = target_directory
		self.dry_run = dry_run
		LOG.info("Validating config")
		self.config.validate_config(CONFIGSPEC)

	def estimate_backup_size(self):
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(self.config['directory']):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# verify the symlink and such exist before trying to get its size
				if os.path.exists(fp):
					total_size += os.path.getsize(fp)
		return total_size

	def backup(self):
		if self.dry_run:
			return
		out_name = "{0}.tar.gz".format(self.config['directory'].replace('/', '_'))
		outfile = os.path.join(self.target_directory, outname)
		args = ['tar', '-xvzf', self.config['directory'], outfile]
		errlog = TemporaryFile()
		LOG.info("Executing: %s", subprocess.list2cmdline(args))
		pid = subprocess.Popen(args,
      stderr=errlog.fileno(),
      close_fds=True)
		status = pid.wait()
		try:
			errlog.flush()
			errlog.seek(0)
			for line in errlog:
				LOG.error("%s[%d]: %s", self.cmd_path, pid.pid, line.rstrip())
		finally:
			errlog.close()
