import os, os.path, subprocess, sys, time
from optparse import OptionParser

def waitUntilDone(jobID, sleep=1):
    while True:
        output = subprocess.Popen("qstat %i"%jobID, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if "Unknown Job" in output[1]:
            break
        time.sleep(sleep)
        
def launchJob(cmd, scriptOptions, verbose=False, test=False):
    scriptOptions["outf"] = os.path.abspath(os.path.join(scriptOptions["outdir"], outscriptName+".out"))
    if type(cmd) not in [type(list()), type(tuple())]:
        cmd = [cmd]

    print cmd
    scriptOptions["command"] = " ".join(cmd)

#    outscriptPath = "/tmp/%s.sh"%outscriptName
#    outscript = open(outscriptPath, "w")

    outtext = """#!/bin/bash

    #PBS -l nodes=%(nodes)s:ppn=%(ppn)s
    #PBS -j oe
    #PBS -o %(outf)s

    #PBS -m a
    #PBS -M nspies@mit.edu
    #PBS -N %(jobname)s

    #PBS -S /bin/bash

    echo $HOSTNAME

    echo Working directory is %(workingdir)s
    cd %(workingdir)s

    echo "%(command)s"
    %(command)s""" % scriptOptions

    if verbose:
        print outscriptName
        print outtext

#    outscript.write(outtext)
#    outscript.close()
    
#    call = "qsub %s"%outscript.name
    call = "qsub -"
    
    try:
        if not test:
            if options.verbose:
                #print "RUNNING:", outscript.name
                print "CALL:", call

            qsub = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            qsub.stdin.write(outtext)
            #qsub.stdin.close()
            output = qsub.communicate()

            if output[0].strip().endswith(".coyote.mit.edu"):
                jobID = int(output[0].split(".")[0])

                if verbose:
                    print "Process launched with job ID:", jobID

                return jobID
            else:
                raise Exception("Failed to launch job: %s"%str(output))
    except:
        print "failing..."
        raise
    finally:
        try:
            os.remove(outscriptPath)
        except:
            pass
    return None

    
if __name__ == "__main__":
    usage = "%prog [options] command"
    parser = OptionParser(usage=usage)
    #parser.add_option("-c", "--command", dest="command", help="Command to run.", metavar="CMD")
    parser.add_option("-d", "--dir", dest="workingdir", help="Working directory (default: current)", default=os.getcwd())
    parser.add_option("-n", "--nodes", dest="nodes", help="Number of nodes or a comma-separated list of nodes to launch to", default="1")
    parser.add_option("-p", "--ppn", dest="ppn", help="Requested number of processors (per node) (default: 1)", default="1")
    parser.add_option("-j", "--jobname", dest="jobname", help="Job name (default: Mypbm.PID)", default=os.path.basename(sys.argv[0]))
    parser.add_option("-q", "--queue", dest="queue", help="Queue to submit to (default: short)", default="short")
    parser.add_option("-o", "--outdir", dest="outdir", help="Directory to save output file to (default:)", default="")
    parser.add_option("-v", "--verbose", dest="verbose", help="Show output script on command line", default=False,
                      action="store_true")
    parser.add_option("-t", "--test", dest="test", help="Write script file but don't run it", default=False,
                      action="store_true")
    parser.add_option("-w", "--wait", dest="wait", help="Wait until job has finished", default=False,
                      action="store_true")
    parser.add_option("-f", "--fast", dest="fast", help="Only submit jobs to the fast nodes; shouldn't be "+\
                          "specified at the same time as -n.", default=False,
                      action="store_true")
    
    (options, args) = parser.parse_args()

    if len(args) == 0:
        parser.error("Need to define command to run")

    pid = os.getpid()
    outscriptName = "%s.%i"%(options.jobname, pid)


    scriptOptions = vars(options)
            
    if options.fast and options.nodes != "1":
        raise Exception("Can only specify either -n or -f.")
    if options.fast:
        scriptOptions["nodes"] = "1:E5450"
        

    jobID = launchJob(args, scriptOptions, options.verbose, options.test)

    if jobID!=None and options.wait:
        waitUntilDone(jobID)



