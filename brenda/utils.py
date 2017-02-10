# Brenda -- Render farm tool for Amazon Web Services
# Copyright (C) 2013 James Yonan <james@openvpn.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os, subprocess, shutil, logging

def config_file_name():
    config = os.environ.get("BRENDA_CONFIG")
    if not config:
        home = os.path.expanduser("~")
        config = os.path.join(home, ".brenda.conf")
    return config

def get_work_dir(conf):
    work_dir = os.path.realpath(conf.get('WORK_DIR', '.'))
    if not os.path.isdir(work_dir):
        makedirs(work_dir)
    return work_dir

def system(cmd, ignore_errors=False):
    logging.info('Execute command: %s', cmd)
    succeed = 0
    ret = subprocess.call(cmd)
    if not ignore_errors and ret != succeed:
        raise ValueError("command failed with status %r (expected %r)" % (ret, succeed))

def rmtree(dir):
    logging.debug('Delete folder: %s', dir)
    shutil.rmtree(dir, ignore_errors=True)

def rm(file):
    logging.debug('Delete file: %s', file)
    try:
        os.remove(file)
    except:
        pass

def mkdir(dir):
    logging.debug('Create folder: %s', dir)
    os.mkdir(dir)

def makedirs(dir):
    logging.debug('Create folders: %s', dir)
    os.makedirs(dir)

def mv(src, dest):
    logging.debug('Move %s to %s', src, dest)
    shutil.move(src, dest)

def shutdown():
    logging.info('Shutdown system')
    system(["/sbin/shutdown", "-h", "0"])

def write_atomic(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        f.write(data)
    os.rename(tmp, path)

def str_nl(s):
    if len(s) > 0 and s[-1] != '\n':
        s += '\n'
    return s

def top_dir(dir):
    """
    If dir contains no files and only one directory,
    return that directory.  Otherwise return dir.
    Note file/dir ignore rules.
    """
    def ignore(fn):
        return fn == 'lost+found' or fn.startswith('.') or fn.endswith('.etag')
    for dirpath, dirnames, filenames in os.walk(dir):
        dirs = []
        for f in filenames:
            if not ignore(f):
                break
        else:
            for d in dirnames:
                if not ignore(d):
                    dirs.append(d)
        if len(dirs) == 1:
            return os.path.join(dirpath, dirs[0])
        else:
            return dirpath

def system_return_output(cmd, capture_stderr=False):
    output = ""
    error = ""
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        if capture_stderr:
            error = e.output
    return str_nl(output) + str_nl(error)

def setup_logger(opts, conf):
    """
    Log to stderr and optional logfile
    """
    log_level = get_opt(opts.loglevel, conf, 'LOG_LEVEL', 'INFO')
    log_file  = get_opt(opts.logfile, conf, 'LOG_FILE', False)
    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    
    # Parse log level
    num_level = getattr(logging, log_level.upper(), None)
    if not isinstance(num_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    
    logging.basicConfig(format=log_format, level=num_level)

    # Toggle Boto logging
    if conf.get('LOG_BOTO', 'FALSE').upper() == 'FALSE':
        logging.getLogger('boto').propagate = False

    # Log to file
    if log_file:
        file_handler = logging.handlers.WatchedFileHandler(log_file, delay=True)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)

    logging.debug('Effective log level: %d', logging.getLogger().getEffectiveLevel())

def get_opt(opt, conf, conf_key, default=None, must_exist=False):
    def g():
        if opt:
            return opt
        else:
            ret = conf.get(conf_key, default)
            if not ret and must_exist:
                raise ValueError("config key %r is missing" % (conf_key,))
            return ret
    ret = g()
    if ret == '*':
        if must_exist:
            raise ValueError("config key %r must not be wildcard" % (conf_key,))
        return None
    return ret

class Cd(object):
    """
    Cd is a context manager that allows
    you to temporary change the working directory.

    with Cd(dir) as cd:
        ...
    """

    def __init__(self, directory):
        self._dir = directory

    def orig(self):
        return self._orig

    def dir(self):
        return self._dir

    def __enter__(self):
        self._orig = os.getcwd()
        os.chdir(self._dir)
        return self

    def __exit__(self, *args):
        os.chdir(self._orig)
