######################################################
#      Taxonamy binning using FR-HIT output          #
######################################################
import os,sys
import re
import cPickle
from optparse import OptionParser

VERSION = '1.1.1'

# Path for tax.pkl and bacteria_gitax.pkl,
# Default is the same as this script
TAXLIB = os.path.dirname(os.path.realpath(__file__))
# GI to Taxonomy ID mapping file:
GITAXFILE = 'bacteria_gitax.pkl'
# Taxonomy tree file
TAXFILE = 'tax.pkl'

### Functions
def commonLineage(lin1,lin2):
    len1 = len(lin1)
    len2 = len(lin2)
    if not len1*len2:
        return -1
    minlen = min(len1,len2)
    for i in range(minlen-1):
        if (lin1[i] == lin2[i]) and (lin1[i+1] != lin2[i+1]):
            return lin1[:i+1]
    return lin1[:minlen]

def gigroup2lineagegroup(gigroup):
    taxidgroup = []
    for gi in gigroup:
        if not gitax.has_key(gi):
            print >>sys.stderr, '!Warning: Cannot map GI',gi,'to a taxonomy ID'
            continue
        taxid = gitax[gi]
        if taxid <= 0: continue
        taxidgroup.append(taxid)
    taxidgroup = list(set(taxidgroup))
    lineagegroup = []
    for taxid in taxidgroup:
        lin = taxid2lineage(taxid)
        if not lin: continue
        if lin[1] == 12908: continue #ignore "unclassified sequences"
        if lin[1] == 28384: continue #ignore "other sequences"
        lineagegroup.append(lin)
    return lineagegroup

def gigroup2commonLineage(gigroup,cutoff=1):
    commonlineage = []
    lineagegroup = gigroup2lineagegroup(gigroup)
    if not lineagegroup: return commonlineage
    minlength = min(tuple(len(l) for l in lineagegroup))
    for i in range(minlength):
        lineagesets = {}
        for l in lineagegroup:
            if lineagesets.has_key(l[i]):
                lineagesets[l[i]].append(l)
            else:
                lineagesets[l[i]] = [l]
        maxsetsize = 0
        maxsetkey = ''
        for s in lineagesets:
            setsize = len(lineagesets[s])
            if setsize > maxsetsize:
                maxsetsize = setsize
                maxsetkey = s
        if float(maxsetsize)/len(lineagegroup) < cutoff:
            return lineagegroup[0][:i]
        lineagegroup = lineagesets[maxsetkey]
    return lineagegroup[0][:minlength]

def taxid2lineage(taxid):
    if taxid <= 0:
        return []
    lin = [taxid]
    current = taxid
    while current != 1:
        if not tax.has_key(current):
            print >>sys.stderr, '!Warning: Unkown taxid:',current
            return []
        parent = tax[current][0]
        lin.append(parent)
        current = parent
    lin.reverse()
    return lin

def printResult(readname,lin):
    if not lin:
        return readname+'\tUNKNOWN'
    r = []
    for taxid in lin:
        name = tax[taxid][2]
        rank = tax[taxid][1]
        r.append((str(taxid),name,rank))
    rr = (i[0]+':'+i[1]+':'+i[2] for i in r)
    return readname+'\t('+r[-1][0]+':'+r[-1][1]+':'+r[-1][2]+')\t['+'|'.join(rr)+']'

### Main #######################################

## parse options
usage = "python %prog [options] input_file(frhit output) output_file"
parser = OptionParser(usage = usage, version="%prog "+VERSION)
parser.add_option("-t","--target",dest="target_read",help="Target a single read by read name")
parser.add_option("-c","--binningcutoff",dest="BINNING_CUTOFF",type="float",default=0.2,help="Cutoff for binning. [0,1], default=0.2")
parser.add_option("-i","--identitycutoff",dest="IDENTITY_CUTOFF",type="float",default=0.8,help="Cutoff for query/hit sequence identity. [0,1], default=0.8")
(options,args) = parser.parse_args()

## Open files
if len(args) != 2:
    parser.print_help()
    sys.exit(1)

infile = args[0]
if not os.path.exists(infile):
    print >>sys.stderr,'Input file',infile,'not found'
    sys.exit(3)

outfile = args[1]
try:
    OUT = open(outfile,'w')
except:
    print >>sys.stderr,'Fail to open output file',outfile
    sys.exit(4)

## Read in taxonomy libraries
pickle_file = open(os.path.join(TAXLIB,GITAXFILE),'rb')
gitax = cPickle.load(pickle_file)
pickle_file = open(os.path.join(TAXLIB,TAXFILE),'rb')
tax = cPickle.load(pickle_file)

## Binning
p = re.compile(r"\bgi\|(\d+)\|")
currentread = ''
gigroup = []
for ln in open(infile):
    ln = ln.strip()
    if not ln: continue
    items = ln.split()
    hitname = items[8]
    m = p.search(hitname)
    if not m: continue
    gi = m.groups()[0]
    if not gi.isdigit():
        print >>sys.stderr,'!Invalid gi:',gi
        continue
    gi = int(gi)
    readname = items[0]
    ident = float(items[7][:-1])/100
    if options.target_read:
        if readname != options.target_read: continue
    if ident < options.IDENTITY_CUTOFF: continue
    if not currentread:
        currentread = readname
        gigroup.append(gi)
    elif currentread == readname:
        gigroup.append(gi)
    elif gigroup:
        finalCommonLineage = gigroup2commonLineage(gigroup,cutoff=options.BINNING_CUTOFF) 
        print >>OUT, printResult(currentread,finalCommonLineage)
        currentread = readname
        gigroup = [gi]
if gigroup: ##last read
    finalCommonLineage = gigroup2commonLineage(gigroup,cutoff=options.BINNING_CUTOFF)
    print >>OUT, printResult(currentread,finalCommonLineage)
