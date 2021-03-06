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

import time, datetime, calendar, urllib2, logging
import boto, boto.sqs, boto.s3, boto.ec2
import boto.utils
from brenda.error import ValueErrorRetry

def get_s3_conn(conf):
    region = conf.get('S3_REGION')
    if region:
        conn = boto.s3.connect_to_region(region, profile_name=conf.get('CREDENTIAL_PROFILE'))
        if not conn:
            raise ValueErrorRetry("Could not establish S3 connection to region %r" % (region,))
    else:
        conn = boto.connect_s3(profile_name=conf.get('CREDENTIAL_PROFILE'))
    return conn

def get_sqs_conn(conf):
    region = conf.get('SQS_REGION')
    if region:
        conn = boto.sqs.connect_to_region(region, profile_name=conf.get('CREDENTIAL_PROFILE'))
        if not conn:
            raise ValueErrorRetry("Could not establish SQS connection to region %r" % (region,))
    else:
        conn = boto.connect_sqs(profile_name=conf.get('CREDENTIAL_PROFILE'))
    return conn

def get_ec2_conn(conf):
    region = conf.get('EC2_REGION')
    if region:
        conn = boto.ec2.connect_to_region(region, profile_name=conf.get('CREDENTIAL_PROFILE'))
        if not conn:
            raise ValueErrorRetry("Could not establish EC2 connection to region %r" % (region,))
    else:
        conn = boto.connect_ec2(profile_name=conf.get('CREDENTIAL_PROFILE'))
    return conn

def parse_s3_url(url):
    if url.startswith('s3://'):
        return url[5:].split('/', 1)

def put_s3_file(bucktup, path, s3name):
    """
    bucktup is the return tuple of get_s3_output_bucket_name
    """
    k = boto.s3.key.Key(bucktup[0])
    k.key = bucktup[1][1] + s3name
    k.set_contents_from_filename(path)

def format_s3_url(bucktup, s3name):
    """
    bucktup is the return tuple of get_s3_output_bucket_name
    """
    return "s3://%s/%s%s" % (bucktup[1][0], bucktup[1][1], s3name)

def get_s3_output_bucket_name(conf):
    bn = conf.get('OUTPUT_URL')
    if not bn:
        raise ValueError("OUTPUT_URL not defined in configuration")
    bn = parse_s3_url(bn)
    if not bn:
        raise ValueError("OUTPUT_URL must be an s3:// URL")
    if len(bn) == 1:
        bn.append('')
    elif len(bn) == 2 and bn[1] and bn[1][-1] != '/':
        bn[1] += '/'
    return bn

def get_s3_output_bucket(conf):
    bn = get_s3_output_bucket_name(conf)
    conn = get_s3_conn(conf)
    buck = conn.get_bucket(bn[0])
    return buck, bn

def parse_sqs_url(url):
    if url.startswith('sqs://'):
        return url[6:]

def get_sqs_work_queue_name(conf):
    qname = conf.get('WORK_QUEUE')
    if not qname:
        raise ValueError("WORK_QUEUE not defined in configuration")
    qname = parse_sqs_url(qname)
    if not qname:
        raise ValueError("WORK_QUEUE must be an sqs:// URL")
    return qname

def create_sqs_queue(conf):
    visibility_timeout = int(conf.get('VISIBILITY_TIMEOUT', '120'))
    message_retention = int(conf.get('MESSAGE_RETENTION', '1209600')) # 14 days
    qname = get_sqs_work_queue_name(conf)

    conn = get_sqs_conn(conf)
    queue = conn.create_queue(qname, visibility_timeout=visibility_timeout)
    conn.set_queue_attribute(queue, 'MessageRetentionPeriod', message_retention)
    return queue

def get_sqs_conn_queue(conf):
    qname = get_sqs_work_queue_name(conf)
    conn = get_sqs_conn(conf)
    q = conn.get_queue(qname)
    if not q:
        logging.error('Queue %s does not exist', qname)
    else:
        logging.debug(conn.get_queue_attributes(q, 'QueueArn')['QueueArn'])

    return q, conn

def write_sqs_queue(string, queue, attributes=None):
    m = boto.sqs.message.Message()
    m.set_body(string)
    if isinstance(attributes, dict):
        m.message_attributes = attributes
    queue.write(m)

def write_batch_sqs_queue(messages, queue):
    return queue.write_batch(messages)

def get_ec2_instances_from_conn(conn, instance_ids=None, filters=None):
    return conn.get_only_instances(instance_ids=instance_ids,filters=filters)

def get_ec2_instances(conf, instance_ids=None, filters=None):
    conn = get_ec2_conn(conf)
    return get_ec2_instances_from_conn(conn, instance_ids, filters)

def format_uptime(sec):
    return str(datetime.timedelta(seconds=sec))

def get_uptime(now, aws_launch_time):
    lt = boto.utils.parse_ts(aws_launch_time)
    return int(now - calendar.timegm(lt.timetuple()))

def filter_instances(opts, conf, filters={}):
    def threshold_test(aws_launch_time):
        ut = get_uptime(now, aws_launch_time)
        return (ut / 60) % 60 >= opts.threshold
    def get_filter_hosts(opts):
        hosts = []
        if getattr(opts, 'hosts_file', None):
            with open(opts.hosts_file, 'r') as f:
                hosts = [line.strip() for line in f.readlines()]
        if getattr(opts, 'host', None):
            hosts.append(opts.host)
        return hosts

    now = time.time()
    if opts.tags:
        filters.update(get_filter_tags(dict(opts.tags)))
    if opts.imatch:
        filters['instance-type'] = opts.imatch
    hosts = get_filter_hosts(opts)
    if len(hosts) > 0:
        filters['dns-name'] = hosts
    logging.debug('Instance filters: %s', filters)
    inst = [i for i in get_ec2_instances(conf, filters=filters) if threshold_test(i.launch_time)]
    inst.sort(key = lambda i : (i.image_id, i.launch_time, i.public_dns_name))
    return inst

def shutdown_by_public_dns_name(opts, conf, dns_names):
    iids = []
    for i in get_ec2_instances(conf):
        if i.public_dns_name in dns_names:
            iids.append(i.id)
    shutdown(opts, conf, iids)

def shutdown(opts, conf, iids):
    # Note that persistent spot instances must be explicitly cancelled,
    # or EC2 will automatically requeue the spot instance request
    if not iids:
        return logging.debug('Shutdown: No instances specified')
    conn = get_ec2_conn(conf)
    if opts.terminate:
        logging.info('Terminate EC2 instance: %s', iids)
        cancel_spot_requests_from_instance_ids(conn, iids, opts.dry_run)
        conn.terminate_instances(instance_ids=iids, dry_run=opts.dry_run)
    else:
        logging.info('Shutdown EC2 instance: %s', iids)
        cancel_spot_requests_from_instance_ids(conn, iids, opts.dry_run)
        conn.stop_instances(instance_ids=iids, dry_run=opts.dry_run)

def get_instance_id_self():
    req = urllib2.Request("http://169.254.169.254/latest/meta-data/instance-id")
    response = urllib2.urlopen(req)
    the_page = response.read()
    return the_page

def get_all_spot_instance_requests(opts, conf, filters={}):
    ec2 = get_ec2_conn(conf)
    filters.update(get_filter_tags(dict(opts.tags)))
    logging.debug('Spot request filters: %s', filters)
    return ec2.get_all_spot_instance_requests(filters=filters)

def get_spot_request_dict(conf):
    ec2 = get_ec2_conn(conf)
    requests = ec2.get_all_spot_instance_requests()
    return dict([(sir.id, sir) for sir in requests])

def get_spot_request_from_instance_id(conf, iid):
    instances = get_ec2_instances(conf, instance_ids=(iid,))
    if instances:
        return instances[0].spot_instance_request_id

def cancel_spot_request(conf, sir):
    conn = get_ec2_conn(conf)
    conn.cancel_spot_instance_requests(request_ids=(sir,))

def cancel_spot_requests_from_instance_ids(conn, instance_ids, dry_run=False):
    instances = get_ec2_instances_from_conn(conn, instance_ids=instance_ids)
    sirs = [ i.spot_instance_request_id for i in instances if i.spot_instance_request_id ]
    logging.info('Cancel spot request: %s', sirs)
    if sirs:
        conn.cancel_spot_instance_requests(request_ids=sirs,dry_run=dry_run)

def get_filter_tags(tags):
    return dict(map(lambda (key, value): ('tag:'+key, value), tags.items()))
