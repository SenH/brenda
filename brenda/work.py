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

import os, sys, random, logging
from brenda import aws

def subframe_iterator_defined(opts):
    return opts.subdiv_x > 0 and opts.subdiv_y > 0

def subframe_iterator(opts):
    if subframe_iterator_defined(opts):
        xfrac = 1.0 / opts.subdiv_x
        yfrac = 1.0 / opts.subdiv_y
        for x in xrange(opts.subdiv_x):
            min_x = x * xfrac
            max_x = (x+1) * xfrac
            for y in xrange(opts.subdiv_y):
                min_y = y * yfrac
                max_y = (y+1) * yfrac
                yield (
                    ('$SF_MIN_X', str(min_x)),
                    ('$SF_MAX_X', str(max_x)),
                    ('$SF_MIN_Y', str(min_y)),
                    ('$SF_MAX_Y', str(max_y)),
                    )

def push(opts, args, conf):
    # get task script
    if not opts.task_script:
        logging.error('-T, --task_script option is required')
    
    try:
        with open(opts.task_script) as f:
            task_script = f.read()
    except Exception:
        logging.error("Could not read task_script file")
        sys.exit(1)

    # check for shebang
    try:
        task_script.startswith("#!")
    except Exception:
        logging.error('Shebang (#!) is missing from task script: %s', opts.task_script)
        sys.exit(1)

    # build tasklist
    tasklist = []
    for fnum in xrange(opts.start, opts.end+1, opts.step):
        script = task_script
        start = fnum
        end = min(fnum + opts.step - 1, opts.end)
        for key, value in (
            ("$JOB_NAME", conf.get("JOB_NAME", "NONE")),
            ("$JOB_URL", conf.get("JOB_URL", "NONE")),
            ("$START", "%d" % (start,)),
            ("$END", "%d" % (end,)),
            ("$STEP", "%d" % (opts.step,))
        ):
            script = script.replace(key, value)
        if subframe_iterator_defined(opts):
            for macro_list in subframe_iterator(opts):
                sf_script = script
                for key, value in macro_list:
                    sf_script = sf_script.replace(key, value)
                tasklist.append(sf_script)
        else:
            tasklist.append(script)

    # possibly randomize the task list
    if opts.randomize:
        random.shuffle(tasklist)

    # get work queue
    q = None
    if not opts.dry_run:
        aws.create_sqs_queue(conf)
        q, conn = aws.get_sqs_conn_queue(conf)

    # push work queue to sqs
    i = 0
    j = 0
    batch = []
    tasklist_last_index = len(tasklist)-1
    for task in tasklist:
        i += 1
        j += 1
        logging.debug("Creating task #%04d: %s", j, task.replace("\n"," "))
        attr = {"script_name": {"data_type": "String", "string_value": os.path.basename(opts.task_script)}}
        batch.append((str(i), task, 0, attr))

        # Deliver up to 10 messages in a single request
        # http://boto.cloudhackers.com/en/latest/ref/sqs.html#boto.sqs.queue.Queue.write_batch
        if i%10 == 0 or tasklist.index(task) == tasklist_last_index:
            if q and not opts.dry_run:
                logging.info('Queueing tasks %d of %d', j, tasklist_last_index+1)
                aws.write_batch_sqs_queue(batch, q)
            del batch[:]
            i = 0

def status(opts, args, conf):
    q, conn = aws.get_sqs_conn_queue(conf)

    if q:
        logging.info("%d tasks queued on %s", q.count(), aws.get_sqs_work_queue_name(conf))

def reset(opts, args, conf):
    q, conn = aws.get_sqs_conn_queue(conf)

    if q:
        if opts.hard:
            logging.info('Deleting queue %s', aws.get_sqs_work_queue_name(conf))
            conn.delete_queue(q)
        else:
            logging.info('Clearing queue %s', aws.get_sqs_work_queue_name(conf))
            q.clear()
