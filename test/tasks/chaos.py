import sys, optparse, time, random

def generate_output_frame(opts, timestamp):
    for i in xrange(opts.start, opts.end+1, opts.step):
        # generate fake frame
        fn = opts.out.replace("######", "{:06d}_{}.txt").format(i, timestamp)
        # print fn
        with open(fn, 'w') as f:
            f.write("This is a test, frame #%d\n" % (i,))

# Parse arguments
parser = optparse.OptionParser()
parser.add_option("-o", "--out", dest="out", help="output file")
parser.add_option("-s", "--start", type="int", dest="start", help="start frame")
parser.add_option("-e", "--end", type="int", dest="end", help="end frame")
parser.add_option("-j", "--step", type="int", dest="step", help="frame increment")
(opts, args) = parser.parse_args()

timestamp = int(time.time())
print '--- Chaos Monkey {} ---'.format(timestamp)
print "Options: start={} end={} step={} out={}".format(opts.start, opts.end, opts.step, opts.out)

# Randomize sleep
rand_int = random.randint(30, 60)
print 'Sleeping {} seconds'.format(rand_int)
time.sleep(rand_int)

# Randomize output
rand_output = random.choice([True, False])
print 'Generating output frame: {}'.format(rand_output)
if rand_output:
    generate_output_frame(opts, timestamp)

# Randomize exit
rand_exit = random.randint(0, 2)
print 'Exiting with status {}'.format(rand_exit)
sys.exit(rand_exit)
