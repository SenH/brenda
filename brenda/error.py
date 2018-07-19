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

import time, httplib, socket, logging, sys
import boto.exception

class ValueErrorRetry(ValueError):
    """
    This exception should be raised for recoverable
    errors, where there is a reasonable assumption that
    retrying the operation will succeed.
    """
    pass

def retry(conf, action):
    n_retries = int(conf.get('ERROR_RETRIES', '5'))
    reset_period = int(conf.get('ERROR_RESET', '3600'))
    error_pause = int(conf.get('ERROR_PAUSE', '30'))

    reset = int(time.time())
    i = 0
    while True:
        try:
            logging.debug('Retrying error action: %s', action)
            ret = action()
        # These are the exception types that justify a retry -- extend this list as needed
        except (httplib.IncompleteRead, socket.error, boto.exception.BotoClientError, ValueErrorRetry), e:
            now = int(time.time())
            if now > reset + reset_period:
                logging.info('Reset error retry')
                i = 0
                reset = now
            i += 1
            logging.warning('Retry error %d/%d: %s', i, n_retries, e)
            if i < n_retries:
                logging.info('Waiting %d seconds before retrying error', error_pause)
                time.sleep(error_pause)
            else:
                logging.critical("Failed after %d error retries", n_retries, exc_info=1)
                sys.exit(1)

        else:
            return ret

def handle_dry_run(e):
    if e.error_code == 'DryRunOperation':
        logging.warning(e.message)
    else:
        logging.error('%s %s %s - %s', e.__class__, e.status, e.reason, e.message)