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

import os, sys, signal, subprocess, multiprocessing, stat, time, logging
import paracurl
from brenda import aws, utils, error

class State(object):
    pass

# We use subprocess.Popen (for render task) and
# multiprocessing.Process (for S3-push task) polymorphically,
# so add some methods to make them consistent.

class Subprocess(subprocess.Popen):
    def stop(self):
        self.terminate()
        return self.wait()

class Multiprocess(multiprocessing.Process):
    def stop(self):
        if self.is_alive():
            self.terminate()
            self.join()
        return self.exitcode

    def poll(self):
        return self.exitcode

def start_s3_push_process(opts, args, conf, outdir):
    p = Multiprocess(target=s3_push_process, args=(opts, args, conf, outdir))
    p.start()
    return p

def s3_push_process(opts, args, conf, outdir):
    def do_s3_push():
        bucktup = aws.get_s3_output_bucket(conf)
        for dirpath, dirnames, filenames in os.walk(outdir):
            for f in filenames:
                path = os.path.join(dirpath, f)
                logging.info('Uploading %s to %s', path, aws.format_s3_url(bucktup, f))
                aws.put_s3_file(bucktup, path, f)
            break

    try:
        error.retry(conf, do_s3_push)
    except Exception:
        logging.exception('Upload to S3 failed')
        sys.exit(1)
    sys.exit(0)

def run_tasks(opts, args, conf):
    def write_done_file():
        with open(os.path.join(work_dir, "DONE"), "w") as f:
            f.write(get_done(opts, conf)+'\n')

    def read_done_file():
        try:
            with open(os.path.join(work_dir, "DONE")) as f:
                ret = f.readline().strip()
        except:
            ret = 'exit'
        validate_done(ret)
        return ret

    def task_complete_accounting(task_count):
        # update some info files if we are running in daemon mode
        # number of tasks we have completed so far
        utils.write_atomic(os.path.join(work_dir, 'task_count'), "%d\n" % (task_count,))

        # timestamp of completion of last task
        utils.write_atomic(os.path.join(work_dir, 'task_last'), "%d\n" % (time.time(),))

    def signal_handler(signal, frame):
        logging.warning("Exit on signal %r", signal)
        cleanup_all()
        sys.exit(1)

    def cleanup_all():
        tasks = (local.task_active, local.task_push)
        local.task_active = local.task_push = None
        for i, task in enumerate(tasks):
            name = task_names[i]
            cleanup(task, name)

    def cleanup(task, name):
        if task:
            if task.msg is not None:
                try:
                    logging.debug('Returning render task \"%s #%s\" back to SQS queue', task.script_name, task.id)
                    msg = task.msg
                    task.msg = None
                    msg.change_visibility(0) # immediately return task back to work queue
                except Exception:
                    logging.exception('Failed changing SQS message visibility of task %s', name)
            if task.proc is not None:
                try:
                    logging.debug('Stopping processing of render task: %s #%s', task.script_name, task.id)
                    proc = task.proc
                    task.proc = None
                    proc.stop()
                except Exception:
                    logging.exception('Failed stopping processing of task %s', name)
            if task.outdir is not None:
                try:
                    outdir = task.outdir
                    task.outdir = None
                    utils.rmtree(outdir)
                except Exception:
                    logging.exception('Failed removing task dir %s', task.outdir)

    def task_loop():
        try:
            # reset tasks
            local.task_active = None
            local.task_push = None

            # get SQS work queue
            q = aws.get_sqs_conn_queue(conf)[0]

            # Loop over tasks.  There are up to two different tasks at any
            # given moment that we are processing concurrently:
            #
            # 1. Active task -- usually a render operation.
            # 2. S3 push task -- a task which pushes the products of the
            #                    previous active task (such as rendered
            #                    frames) to S3.
            while True:
                # reset active task
                local.task_active = None

                # initialize active task object
                task = State()
                task.msg = None
                task.proc = None
                task.retcode = None
                task.outdir = None
                task.id = 0
                task.script_name = None

                # Get a task from the SQS work queue.  This is normally
                # a short script that renders one or more frames.
                task.msg = q.read(message_attributes=['All'])

                # output some debug info
                logging.debug('Reading work queue')
                if local.task_push:
                    logging.info("Running upload task #%d", local.task_push.id)
                    logging.debug(local.task_push.__dict__)
                else:
                    logging.info('No upload task available')

                # process task
                if task.msg is not None:
                    # assign an ID to task
                    local.task_id_counter += 1
                    task.id = local.task_id_counter
                    task.script_name = task.msg.message_attributes['script_name']['string_value']

                    # register active task
                    local.task_active = task

                    # create output directory
                    task.outdir = os.path.join(work_dir, "{}_out_{}".format(task.script_name, task.id))
                    logging.info('Task folder: %s', task.outdir);
                    utils.rmtree(task.outdir)
                    utils.mkdir(task.outdir)

                    # get the task script
                    script = task.msg.get_body()

                    # cd to output directory, where we will run render task from
                    with utils.Cd(task.outdir):
                        # write script file and make it executable
                        script_fn = "./{}".format(task.script_name)
                        with open(script_fn, 'w') as f:
                            f.write(script)
                        st = os.stat(script_fn)
                        os.chmod(script_fn, st.st_mode | (stat.S_IEXEC|stat.S_IXGRP|stat.S_IXOTH))

                        # run the script
                        logging.info("Executing task script: %s", os.path.realpath(script_fn))
                        logging.debug(script.replace("\n"," "))
                        task.proc = Subprocess([script_fn])

                    logging.info('Running render task: %s #%d', local.task_active.script_name, local.task_active.id)
                    logging.debug(local.task_active.__dict__)

                # Wait for active and S3-push tasks to complete,
                # while periodically reasserting with SQS to
                # acknowledge that tasks are still pending.
                # (If we don't reassert with SQS frequently enough,
                # it will assume we died, and put our tasks back
                # in the queue.  "frequently enough" means within
                # visibility_timeout.)
                count = 0
                while True:
                    reassert = (count >= visibility_timeout_reassert)
                    for i, task in enumerate((local.task_active, local.task_push)):
                        if task:
                            name = task_names[i]
                            if task.proc is not None:
                                # test if process has finished
                                task.retcode = task.proc.poll()
                                if task.retcode is not None:
                                    # process has finished
                                    task.proc = None

                                    # did process finish with errors?
                                    if task.retcode != 0:
                                        if name == 'active':
                                            errtxt = "Render task \"{} #{}\" exited with status code {}".format(
                                            task.script_name, task.id, task.retcode)
                                            raise error.ValueErrorRetry(errtxt)
                                        else:
                                            errtxt = "Upload task #{} exited with status code {}".format(task.id, task.retcode)
                                            raise ValueError(errtxt)

                                    # Process finished successfully.  If S3-push process,
                                    # tell SQS that the task completed successfully.
                                    if name == 'push':
                                        logging.info('Finished upload task #%d', task.id)
                                        q.delete_message(task.msg)
                                        task.msg = None
                                        local.task_count += 1
                                        task_complete_accounting(local.task_count)
 
                                    # active task completed?
                                    if name == 'active':
                                        logging.info('Finished render task: %s #%d', task.script_name,task.id)

                            # tell SQS that we are still working on the task
                            if reassert and task.proc is not None:
                                log_task_name = name.replace('push', 'upload').replace('active', 'render')
                                logging.debug('Reasserting %s task %d with SQS', log_task_name, task.id)
                                task.msg.change_visibility(visibility_timeout)

                    # break out of loop only when no pending tasks remain
                    if ((not local.task_active or local.task_active.proc is None)
                        and (not local.task_push or local.task_push.proc is None)):
                        break

                    # setup for next process poll iteration
                    if reassert:
                        count = 0
                    time.sleep(1)
                    count += 1

                # clean up the S3-push task
                cleanup(local.task_push, 'push')
                local.task_push = None

                # start a concurrent push task to commit files generated by
                # just-completed active task (such as render frames) to S3
                if local.task_active:
                    local.task_active.proc = start_s3_push_process(opts, args, conf, local.task_active.outdir)
                    local.task_push = local.task_active
                    local.task_active = None

                # if no active task and no S3-push task, we are done (unless DONE is set to "poll")
                if not local.task_active and not local.task_push:
                    if read_done_file() == "poll":
                        logging.info('Waiting for tasks...')
                        time.sleep(15)
                    else:
                        logging.info('Exiting')
                        break

        finally:
            cleanup_all()

    # initialize task_active and task_push states
    task_names = ('active', 'push')
    local = State()
    local.task_active = None
    local.task_push = None
    local.task_id_counter = 0
    local.task_count = 0

    # setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # get configuration parameters
    work_dir = utils.get_work_dir(conf)
    visibility_timeout_reassert = int(conf.get('VISIBILITY_TIMEOUT_REASSERT', '30'))
    visibility_timeout = int(conf.get('VISIBILITY_TIMEOUT', '120'))

    # validate RENDER_OUTPUT bucket
    aws.get_s3_output_bucket(conf)

    # file cleanup
    utils.rm(os.path.join(work_dir, 'task_count'))
    utils.rm(os.path.join(work_dir, 'task_last'))

    # save the value of DONE config var
    write_done_file()

    # Get our spot instance request, if it exists
    spot_request_id = None
    if int(conf.get('RUNNING_ON_EC2', '1')):
        try:
            instance_id = aws.get_instance_id_self()
            spot_request_id = aws.get_spot_request_from_instance_id(conf, instance_id)
            logging.info('Spot request ID: %s', spot_request_id)
        except Exception:
            logging.exception('Failed getting spot instance request')

    # continue only if we are not in "dry-run" mode
    if not opts.dry_run:
        # execute the task loop
        error.retry(conf, task_loop)

        # if "DONE" file == "shutdown", do a shutdown now as we exit
        if read_done_file() == "shutdown":
            if spot_request_id:
                try:
                    # persistent spot instances must be explicitly cancelled, or
                    # EC2 will automatically requeue the spot instance request
                    logging.info("Canceling spot instance request: %s", spot_request_id)
                    aws.cancel_spot_request(conf, spot_request_id)
                except Exception:
                    logging.exception("Failed canceling spot instance request")
            utils.shutdown()

        logging.info('Completed %d tasks', local.task_count)

def validate_done(d):
    done_choices = ('exit', 'shutdown', 'poll')
    if d not in done_choices:
        raise ValueError("DONE config var must be one of %r" % (done_choices,))

def get_done(opts, conf):
    if getattr(opts, 'shutdown', False):
        return 'shutdown'
    else:
        d = conf.get('DONE')
        if d:
            validate_done(d)
            return d
        else:
            sd = int(conf.get('SHUTDOWN', '0'))
            return 'shutdown' if sd else 'exit'
