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

import os, time, logging
from brenda import aws, node, utils

def demand(opts, conf):
    run_args = {
        'image_id'              : utils.get_opt(opts.ami, conf, 'AMI_ID', must_exist=True),
        'max_count'             : opts.n_instances,
        'instance_type'         : utils.get_opt(opts.instance_type, conf, 'INSTANCE_TYPE', must_exist=True),
        'instance_profile_name' : utils.get_opt(opts.instance_profile, conf, 'INSTANCE_PROFILE', must_exist=True),
        'user_data'             : startup_script(opts, conf) if not opts.idle else None,
        'key_name'              : conf.get("SSH_KEY_NAME", "brenda"),
        'security_groups'       : [conf.get("SECURITY_GROUP", "brenda")],
        'monitoring_enabled'    : opts.monitoring,
        'dry_run'               : opts.dry_run,
        }
    logging.debug('Instance parameters: %s', run_args)
    node.get_done(opts, conf) # sanity check on DONE var

    # Request instances
    ec2 = aws.get_ec2_conn(conf)
    reservation = ec2.run_instances(**run_args)
    logging.info(reservation)
    if not opts.dry_run: tag_demand_resources(conf, reservation, dict(opts.tags))

def spot(opts, conf):
    run_args = {
        'image_id'              : utils.get_opt(opts.ami, conf, 'AMI_ID', must_exist=True),
        'price'                 : utils.get_opt(opts.price, conf, 'BID_PRICE', must_exist=True),
        'type'                  : 'persistent' if opts.persistent else 'one-time',
        'count'                 : opts.n_instances,
        'instance_type'         : utils.get_opt(opts.instance_type, conf, 'INSTANCE_TYPE', must_exist=True),
        'instance_profile_name' : utils.get_opt(opts.instance_profile, conf, 'INSTANCE_PROFILE', must_exist=True),
        'user_data'             : startup_script(opts, conf) if not opts.idle else None,
        'key_name'              : conf.get("SSH_KEY_NAME", "brenda"),
        'security_groups'       : [conf.get("SECURITY_GROUP", "brenda")],
        'monitoring_enabled'    : opts.monitoring,
        'dry_run'               : opts.dry_run,
        }
    logging.debug('Instance parameters: %s', run_args)
    node.get_done(opts, conf) # sanity check on DONE var

    # Request spot instances
    ec2 = aws.get_ec2_conn(conf)
    reservation = ec2.request_spot_instances(**run_args)
    logging.info(reservation)
    if not opts.dry_run: tag_spot_resources(conf, reservation, dict(opts.tags))

def price(opts, conf):
    ec2 = aws.get_ec2_conn(conf)
    itype = utils.get_opt(opts.instance_type, conf, 'INSTANCE_TYPE', must_exist=True)
    data = {}
    for item in ec2.get_spot_price_history(instance_type=itype, product_description="Linux/UNIX"):
        # show the most recent price for each availability zone
        if item.availability_zone in data:
            if item.timestamp > data[item.availability_zone].timestamp:
                data[item.availability_zone] = item
        else:
            data[item.availability_zone] = item

    print "Spot price data for instance", itype
    for k, v in sorted(data.items()):
        print "%s %s $%s" % (v.availability_zone, v.timestamp, v.price)

def stop(opts, conf):
    instances = aws.filter_instances(opts, conf)
    iids = [i.id for i in instances]
    aws.shutdown(opts, conf, iids)

def cancel(opts, conf):
    ec2 = aws.get_ec2_conn(conf)
    requests = aws.get_all_spot_instance_requests(opts, conf, {'state': ['open', 'active']})
    requests = [r.id for r in requests]
    logging.info('Cancel %s', requests)
    ec2.cancel_spot_instance_requests(requests, opts.dry_run)

def status(opts, conf):
    now = time.time()
    instances = aws.filter_instances(opts, conf, {'instance-state-name': 'running'})
    if instances:
        print "Running Instances"
    for i in instances:
        uptime = aws.get_uptime(now, i.launch_time)
        print ' ', i.image_id, aws.format_uptime(uptime), i.public_dns_name, i.tags

    requests = aws.get_all_spot_instance_requests(opts, conf, {'state': ['active', 'open']})
    if requests:
        print "Active Spot Requests"
    for r in requests:
        print "  %s %s %s %s $%s %s %s %s" % (r.id, r.region, r.type, r.create_time, r.price, r.state, r.status, r.tags)

def init(opts, conf):
    ec2 = aws.get_ec2_conn(conf)

    # create ssh key pair
    if not opts.no_ssh_keys:
        # get new ssh public key pair from AWS
        ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
        ssh_local_fn = utils.get_local_ssh_id_fn(opts, conf, mkdir=True)
        logging.info("Creating AWS ssh key pair %r.", ssh_key_name)
        keypair = ec2.create_key_pair(ssh_key_name, opts.dry_run)
        with open(ssh_local_fn, 'w') as f:
            pass
        os.chmod(ssh_local_fn, 0600)
        with open(ssh_local_fn, 'w') as f:
            f.write(keypair.material)
            logging.info("Created local ssh id in %r from %r key pair.", ssh_local_fn, ssh_key_name)

    # create security group
    if not opts.no_security_group:
        sec_group = conf.get("SECURITY_GROUP", "brenda")
        logging.info("Creating Brenda security group %r", sec_group)
        sg = ec2.create_security_group(sec_group, 'Brenda security group', dry_run=opts.dry_run)
        logging.debug('Tagging %s with %s', sg, dict(opts.tags))
        ec2.create_tags(sg.id, dict(opts.tags))
        sg.authorize('tcp', 22, 22, '0.0.0.0/0')  # ssh
        sg.authorize('icmp', -1, -1, '0.0.0.0/0') # all ICMP

def reset(opts, conf):
    ec2 = aws.get_ec2_conn(conf)

    # remove ssh keys
    if not opts.no_ssh_keys:
        ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
        logging.info("Deleting AWS ssh key pair %r.", ssh_key_name)
        ec2.delete_key_pair(ssh_key_name, opts.dry_run)
        ssh_local_fn = utils.get_local_ssh_id_fn(opts, conf)
        if os.path.exists(ssh_local_fn):
            logging.info("Removing local ssh id %r.", ssh_local_fn)
            os.remove(ssh_local_fn)
        else:
            logging.info("Local ssh id %r does not exist", ssh_local_fn)

    # remove security group
    if not opts.no_security_group:
        sec_group = conf.get("SECURITY_GROUP", "brenda")
        logging.info("Removing Brenda security group %r", sec_group)
        ec2.delete_security_group(sec_group, dry_run=opts.dry_run)

def startup_script(opts, conf):
    head = "#!/usr/bin/env bash\n su - " + conf.get("AMI_USER", "root") + " -c "
    head += "/usr/local/bin/brenda-node --daemon <<EOF\n"
    tail = "EOF\n"
    keys = [
        "SQS_REGION",
        "EC2_REGION",
        "S3_REGION",
        'WORK_QUEUE',
        'WORK_DIR',
        'OUTPUT_URL',
        ]
    optional_keys = [
        "LOG_LEVEL",
        "LOG_FILE",
        "LOG_DAEMON",
        "VISIBILITY_TIMEOUT",
        "VISIBILITY_TIMEOUT_REASSERT",
        "ERROR_RETRIES",
        "ERROR_PAUSE",
        "ERROR_RESET",
        "SHUTDOWN",
        "DONE"
        ]

    script = head
    for k in keys:
        v = conf.get(k)
        if not v:
            raise ValueError("config key %r must be defined" % (k,))
        script += "%s=%s\n" % (k, v)
    for k in optional_keys:
        v = conf.get(k)
        if v:
            script += "%s=%s\n" % (k, v)
    script += tail
    return script

def tag_demand_resources(conf, reservation, tags):
    """
    Tag demand resources
    http://boto.cloudhackers.com/en/latest/ref/ec2.html#boto.ec2.instance.Reservation
    """
    ec2 = aws.get_ec2_conn(conf)
    tag_ids = []
    
    # Tag instances
    for instance in reservation.instances:
        tag_ids.extend(get_tag_instance_ids(conf, instance.id))

    logging.info('Tagging %s with %s', tag_ids, tags)
    ec2.create_tags(tag_ids, tags)

def tag_spot_resources(conf, reservation, tags):
    """
    Tag spot resources
    http://boto.cloudhackers.com/en/latest/ref/ec2.html#module-boto.ec2.spotinstancerequest
    """
    ec2 = aws.get_ec2_conn(conf)
    tag_ids = []

    # Tag reservations
    for res in reservation:    
        logging.info('Tagging %s with %s', res, tags)
        ec2.create_tags(res.id, tags)

        # Wait until request is fulfilled
        while True:
            logging.debug('Waiting for spot request to be fulfilled...')
            time.sleep(3)
            spot_request = ec2.get_all_spot_instance_requests(res.id)[0]
            if spot_request.state != 'open':
                break
            
        # Tag instance
        tag_ids.extend(get_tag_instance_ids(conf, spot_request.instance_id))

    logging.info('Tagging %s with %s', tag_ids, tags)
    ec2.create_tags(tag_ids, tags)

def get_tag_instance_ids(conf, instance_id):
    ec2 = aws.get_ec2_conn(conf)
    tag_ids = []

    logging.info('Getting instance %s', instance_id)
    tag_ids.append(instance_id)
    
    # Wait for block volumes
    while True:
        logging.debug('Waiting for block volumes...')
        time.sleep(3)
        inst_attr = ec2.get_instance_attribute(instance_id=instance_id, attribute='blockDeviceMapping')
        if len(inst_attr['blockDeviceMapping']) > 0:
            for block_device in inst_attr['blockDeviceMapping'].itervalues():
                # print block_device.__dict__
                logging.info('Getting block volume %s', block_device.volume_id)
                tag_ids.append(block_device.volume_id)
            break
    
    return tag_ids