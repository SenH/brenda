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
from brenda import aws, utils

def demand(opts, conf):
    ami_id = utils.get_opt(opts.ami, conf, 'AMI_ID', must_exist=True)
    itype = brenda_instance_type(opts, conf)
    snapshots = aws.get_snapshots(conf)
    bdm, snap_description, istore_dev = aws.blk_dev_map(opts, conf, itype, snapshots)
    script = startup_script(opts, conf, istore_dev)
    user_data = None
    if not opts.idle:
        user_data = script
    ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
    sec_groups = (conf.get("SECURITY_GROUP", "brenda"),)
    run_args = {
        'image_id'      : ami_id,
        'max_count'     : opts.n_instances,
        'instance_type' : itype,
        'user_data'     : user_data,
        'key_name'      : ssh_key_name,
        'security_groups' : sec_groups,
        'block_device_map' : bdm,
        'dry_run'       : opts.dry_run,
        }

    print "----------------------------"
    print "AMI ID:", ami_id
    print "Instance type:", itype
    print "Max instances:", opts.n_instances
    if snap_description:
        print "Project EBS snapshot:", snap_description
    if istore_dev:
        print "Instance store device:", istore_dev
    print "SSH key name:", ssh_key_name
    print "Security groups:", sec_groups
    print_script(opts, script)
    aws.get_done(opts, conf) # sanity check on DONE var

    # Request instances
    ec2 = aws.get_ec2_conn(conf)
    logging.debug('Instance parameters: %s', run_args)
    reservation = ec2.run_instances(**run_args)
    if not opts.dry_run:
        logging.info(reservation)
        tag_demand_resources(conf, reservation, dict(opts.tags))

def spot(opts, conf):
    ami_id = utils.get_opt(opts.ami, conf, 'AMI_ID', must_exist=True)
    price = utils.get_opt(opts.price, conf, 'BID_PRICE', must_exist=True)
    reqtype = 'persistent' if opts.persistent else 'one-time'
    itype = brenda_instance_type(opts, conf)
    snapshots = aws.get_snapshots(conf)
    bdm, snap_description, istore_dev = aws.blk_dev_map(opts, conf, itype, snapshots)
    script = startup_script(opts, conf, istore_dev)
    user_data = None
    if not opts.idle:
        user_data = script
    ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
    sec_groups = (conf.get("SECURITY_GROUP", "brenda"),)
    run_args = {
        'image_id'      : ami_id,
        'price'         : price,
        'type'          : reqtype,
        'count'         : opts.n_instances,
        'instance_type' : itype,
        'user_data'     : user_data,
        'key_name'      : ssh_key_name,
        'security_groups' : sec_groups,
        'block_device_map' : bdm,
        'dry_run'       : opts.dry_run,
        }

    print "----------------------------"
    print "AMI ID:", ami_id
    print "Max bid price", price
    print "Request type:", reqtype
    print "Instance type:", itype
    print "Instance count:", opts.n_instances
    if snap_description:
        print "Project EBS snapshot:", snap_description
    if istore_dev:
        print "Instance store device:", istore_dev
    print "SSH key name:", ssh_key_name
    print "Security groups:", sec_groups
    print_script(opts, script)
    aws.get_done(opts, conf) # sanity check on DONE var

    # Request spot instances
    ec2 = aws.get_ec2_conn(conf)
    logging.debug('Instance parameters: %s', run_args)
    reservation = ec2.request_spot_instances(**run_args)
    if not opts.dry_run:
        logging.info(reservation)
        tag_spot_resources(conf, reservation, dict(opts.tags))

def price(opts, conf):
    ec2 = aws.get_ec2_conn(conf)
    itype = brenda_instance_type(opts, conf)
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
    requests = aws.get_all_spot_instance_requests(opts, conf,  {'state': ['open', 'active']})
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

def script(opts, conf):
    itype = brenda_instance_type(opts, conf)
    snapshots = aws.get_snapshots(conf)
    bdm, snap_description, istore_dev = aws.blk_dev_map(opts, conf, itype, snapshots)
    script = startup_script(opts, conf, istore_dev)
    print script

def init(opts, conf):
    ec2 = aws.get_ec2_conn(conf)

    # create ssh key pair
    if not opts.no_ssh_keys:
        try:
            ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
            if not opts.aws_ssh_pull and aws.local_ssh_keys_exist(opts, conf):
                # push local ssh public key to AWS
                pubkey_fn = aws.get_ssh_pubkey_fn(opts, conf)
                logging.info("Importing ssh public key %r to AWS under %r key pair.", pubkey_fn, ssh_key_name)
                with open(pubkey_fn) as f:
                    pubkey = f.read()
                    keypair = ec2.import_key_pair(ssh_key_name, pubkey)
            else:
                # get new ssh public key pair from AWS
                brenda_ssh_ident_fn = aws.get_brenda_ssh_identity_fn(opts, conf, mkdir=True)
                logging.info("Creating ssh private key from AWS into %r under %r key pair.", brenda_ssh_ident_fn, ssh_key_name)
                keypair = ec2.create_key_pair(key_name=ssh_key_name)
                with open(brenda_ssh_ident_fn, 'w') as f:
                    pass
                os.chmod(brenda_ssh_ident_fn, 0600)
                with open(brenda_ssh_ident_fn, 'w') as f:
                    f.write(keypair.material)
        except Exception:
            logging.exception("Failed creating ssh key pair")

    # create security group
    if not opts.no_security_group:
        try:
            sec_group = conf.get("SECURITY_GROUP", "brenda")
            logging.info("Creating Brenda security group %r", sec_group)
            sg = ec2.create_security_group(sec_group, 'Brenda security group')
            logging.debug('Tagging %s with %s', sg, dict(opts.tags))
            ec2.create_tags(sg.id, dict(opts.tags))
            sg.authorize('tcp', 22, 22, '0.0.0.0/0')  # ssh
            sg.authorize('icmp', -1, -1, '0.0.0.0/0') # all ICMP
        except Exception:
            logging.exception("Failed creating Brenda security group")

def reset_keys(opts, conf):
    ec2 = aws.get_ec2_conn(conf)

    # remove ssh keys
    if not opts.no_ssh_keys:
        try:
            ssh_key_name = conf.get("SSH_KEY_NAME", "brenda")
            logging.info("Deleting AWS ssh key pair %r.", ssh_key_name)
            ec2.delete_key_pair(key_name=ssh_key_name)
            brenda_ssh_ident_fn = aws.get_brenda_ssh_identity_fn(opts, conf)
            if os.path.exists(brenda_ssh_ident_fn):
                logging.info("Removing local ssh identity %r.", brenda_ssh_ident_fn)
                os.remove(brenda_ssh_ident_fn)
        except Exception:
            logging.exception("Failed removing ssh key pair")

    # remove security group
    if not opts.no_security_group:
        try:
            sec_group = conf.get("SECURITY_GROUP", "brenda")
            logging.info("Removing Brenda security group %r", sec_group)
            ec2.delete_security_group(name=sec_group)
        except Exception:
            logging.exception("Failed removing Brenda security group")

def startup_script(opts, conf, istore_dev):
    login_dir = "/root"

    head = "#!/bin/bash\n"

    # use EC2 instance store on render farm instance?
    use_istore = int(conf.get('USE_ISTORE', '1' if istore_dev else '0'))

    if use_istore:
        # script to start brenda-node running
        # on the EC2 instance store
        iswd = conf.get('WORK_DIR', '/mnt/brenda')
        if iswd != login_dir:
            head += """\
# run Brenda on the EC2 instance store volume
B="%s"
if ! [ -d "$B" ]; then
  for f in brenda.pid log task_count task_last DONE ; do
    ln -s "$B/$f" "%s/$f"
  done
fi
export BRENDA_WORK_DIR="."
mkdir -p "$B"
cd "$B"
""" % (iswd, login_dir)
        else:
            head += 'cd "%s"\n' % (login_dir,)
    else:
        head += 'cd "%s"\n' % (login_dir,)

    head += "/usr/local/bin/brenda-node --daemon <<EOF\n"
    tail = "EOF\n"
    keys = [
        'AWS_ACCESS_KEY',
        'AWS_SECRET_KEY',
        'RENDER_PROJECT',
        'WORK_QUEUE',
        'RENDER_OUTPUT'
        ]
    optional_keys = [
        "S3_REGION",
        "SQS_REGION",
        "CURL_MAX_THREADS",
        "CURL_N_RETRIES",
        "CURL_DEBUG",
        "VISIBILITY_TIMEOUT",
        "VISIBILITY_TIMEOUT_REASSERT",
        "N_RETRIES",
        "ERROR_PAUSE",
        "RESET_PERIOD",
        "RENDER_PROJECT_ALWAYS_REFETCH",
        "WORK_DIR",
        "SHUTDOWN",
        "DONE"
        ] + list(aws.additional_ebs_iterator(conf))

    script = head
    for k in keys:
        v = conf.get(k)
        if not v:
            raise ValueError("config key %r must be defined" % (k,))
        script += "%s=%s\n" % (k, v)
    for k in optional_keys:
        if k == "WORK_DIR" and use_istore:
            continue
        v = conf.get(k)
        if v:
            script += "%s=%s\n" % (k, v)
    script += tail
    return script

def print_script(opts, script):
    if not opts.idle:
        print "Startup Script:"
        for line in script.splitlines():
            for redact in ('AWS_ACCESS_KEY=', 'AWS_SECRET_KEY='):
                if line.startswith(redact):
                    line = redact + "[redacted]"
                    break
            print '  ', line

def brenda_instance_type(opts, conf):
    return utils.get_opt(opts.instance_type, conf, 'INSTANCE_TYPE', default="m2.xlarge")

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