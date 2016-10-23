#!/usr/bin/python

# Author: Timothy Keller (turbofx@gmail.com)
# Program: Chunkomatic 2.
# Purpose: This program is basically split on steroids.
# Problem: Someone has a 100GB file they need to get from A to B.  However, the connection is slow and noisy.
# This program splits the file into managable chunks and creates a map file that contains sha1sums of the entire file along with
# sha1sums of the individual chunks

# This program does the following:
# 1. Takes a file and generates chunks and a map file
# 2. Takes a map file  chunks and reassembles them into a file
# 3. Takes a map file and simply verfies the chunks

import os, sys, hashlib, ConfigParser
from optparse import OptionParser

DEFAULT_CHUNK_SIZE = 536870912 # 500MB
DEFAULT_BLOCK_SIZE = 32768 # default block read size
DEFAULT_CHUNK_LABEL = 'c'
DEFAULT_HASH_TYPE = 'sha1'

DEBUG = True

def debug(msg):
    if DEBUG:
        print "XX DEBUG XX: ", msg

def error_msg(msg):
    print "Error: %s" % msg

class chunkomatic(object):
    def __init__(self, default_chunk_size=DEFAULT_CHUNK_SIZE, default_block_size = DEFAULT_BLOCK_SIZE):
        self.default_chunk_size = default_chunk_size
        self.default_block_size = default_block_size
        self.default_chunk_label = DEFAULT_CHUNK_LABEL
        self.default_hash_type = DEFAULT_HASH_TYPE

    def setup_mapconfig(self):
        """ 
        Create a ConfigParser object and add our default options 
        """
        self.mapfile_config = ConfigParser.RawConfigParser()
        self.mapfile_config.add_section('Defaults')
        self.mapfile_config.set('Defaults', 'default_chunk_size', self.default_chunk_size)
        self.mapfile_config.set('Defaults', 'default_block_size', self.default_block_size)
        self.mapfile_config.set('Defaults', 'default_chunk_label', self.default_chunk_label)
        self.mapfile_config.set('Defaults', 'default_hash_type', self.default_hash_type)

    def load_defaults(self):
        """
        Read default options from our ConfigParser object
        """
        self.default_chunk_size = self.mapfile_config.getint('Defaults', 'default_chunk_size')
        self.default_block_size = self.mapfile_config.getint('Defaults', 'default_block_size')
        self.default_chunk_label = self.mapfile_config.get('Defaults', 'default_chunk_label')
        self.default_hash_type = self.mapfile_config.get('Defaults', 'default_hash_type')
        return True

    def generate_chunks(self, file2process, create_chunks=True, abspath=False):
        """
        This function takes a file and generates N number of chunks
        The default file name for each chunk is DEFAULT_CHUNK_LABEL_#
        """
        try:
            fp = os.open(os.path.abspath(file2process), os.O_RDONLY)
        except:
            # log error here
            debug("Unable to open file: %s for reading!" % file2process)
            return -1
        if abspath:
            section_name = 'file:%s' % os.path.abspath(file2process)
        else:
            section_name = 'file:%s' % file2process
        
        self.mapfile_config.add_section(section_name)
        fsize = os.stat(os.path.abspath(file2process)).st_size
        self.mapfile_config.set(section_name, 'fsize', fsize)
        fchunks = int(fsize / self.default_chunk_size) + 1
        self.mapfile_config.set(section_name, 'fchunks', fchunks)
        debug("File will generate %d checksum chunks" % fchunks)
        chunk_num = 0 # what chunk we're currently in
        chunk_label = '%s%d' % (self.default_chunk_label, chunk_num) # The zero chunk
        start_offset = 0 # where we started in the file
        cur_offset = 0 # where we are in the file
        chunk_offset = 0 # where we are in the chunk
        chunk_hash = hashlib.new(self.default_hash_type) # Create the hash for the chunk
        total_hash = hashlib.new(self.default_hash_type) # Create the hash for the whole file
        debug("Processing chunk: %d" % chunk_num)
        file_open = False
        while(chunk_num < fchunks):
            if not file_open:
                if create_chunks:
                    debug("Opening chunk file to write: %s" % chunk_label)
                    try:
                        cf = open(chunk_label, 'wb+')
                    except OSError, e:
                        debug("Unable to open file: %s" % cf)
                        debug("Error: %s" % e)
                        exit(-1)
                    file_open = True
                    
            hchunk = os.read(fp, self.default_block_size) # read a chunk from the file
            if len(hchunk) > 0: # we're not at end of file, yet
                if create_chunks:
                    cf.write(hchunk)
                chunk_hash.update(hchunk) # update the hash
                total_hash.update(hchunk) # update the file hash
                cur_offset += len(hchunk) # increment our file offset
                chunk_offset += len(hchunk) # increment where we are in the file
                if chunk_offset >= self.default_chunk_size: # we've hit the end of a chunk
                    debug("Chunk boundary hit! [%d : %s]" % (cur_offset, chunk_hash.hexdigest()))
                    self.mapfile_config.set(section_name, chunk_label, "%d %d %s %s" % 
                                            (start_offset, cur_offset, chunk_hash.hexdigest(), total_hash.hexdigest()))
                    # reset the chunk hash
                    chunk_hash = hashlib.new(self.default_hash_type)
                    chunk_num += 1
                    chunk_label = '%s%d' % (self.default_chunk_label, chunk_num)
                    chunk_offset = 0
                    start_offset = cur_offset # Move the start offset up for the next chunk
                    if create_chunks:
                        cf.close()
                        file_open = False
            else: # len(hchunk) is <= 0 thus EOF
                os.close(fp)
                debug("Last Chunk: [%d : %s]" % (cur_offset, chunk_hash.hexdigest()))
                self.mapfile_config.set(section_name, chunk_label, "%d %d %s %s" % 
                                        (start_offset, cur_offset, chunk_hash.hexdigest(), total_hash.hexdigest())) # write chunk
                self.mapfile_config.set(section_name, 'file_checksum', "%s" % total_hash.hexdigest()) # wrote total file checksum
                break
        debug("Processing of file %s is complete!" % file2process)
        return 0


    def examine(self, filetoexamine):
        """
        Look in the map file, get the list of chunks and check each one
        """
        flist = self.mapfile_config.items('file:' + filetoexamine)
        glist = []
        blist = []
        for x in flist:
            if x[0] == 'fchunks':
                chunks_to_check = int(x[1])
                print "Chucks to check", chunks_to_check
            if x[0][0] == self.default_chunk_label:
                print "Chunk to check: ", x[0]
                if self.check_chunk(x) == False:
                    print "Chunk: %s failed verification" % x[0]
                    blist.append(x[0])
                else:
                    glist.append(x[0])
        if chunks_to_check == len(glist):
            return True
        else:
            print "The following chunk files are corrupt:"
            for x in blist:
                print "File: %s" % x
            return False

    def assemble(self, filetoassemble)
    
    
    def check_chunk(self, chunk_to_check):
        """
        chunk_to_check is a tuple of (name, start_loc, end_loc, sha1 of chuck, sha1 sum to this point)
        """
        chunk_checksum = chunk_to_check[1].split()[2]
        try:
            fp = open(chunk_to_check[0], 'rb')
        except OSError:
            error_msg("Unable to open chunk file: %s" % chunk_to_check[0])
            return False
        chunk_hash = hashlib.new(self.default_hash_type)
        done = False
        debug("Checking chunk: %s" % chunk_to_check[0])
        while not done:
            chunk = fp.read(self.default_chunk_size)
            chunk_hash.update(chunk)
            if len(chunk) < self.default_chunk_size: # we've hit end of file
                done = True
        fp.close()
        debug("Final hash: %s" % chunk_hash.hexdigest())
        if chunk_hash.hexdigest() != chunk_checksum:
            return False
        else:
            return True
        
    
    def write_mapfile(self, mapfile):
        with open(mapfile, 'wb') as mp:
            self.mapfile_config.write(mp)

    def load_mapfile(self, mapfile):
        try:
            fp = open(mapfile, 'rb')
        except:
            debug("Unable to open map file!")
            exit(-1)
        debug("Loading map file: %s" % mapfile)
        self.mapfile_config = ConfigParser.RawConfigParser()
        self.mapfile_config.readfp(fp)
        fp.close()
        self.load_defaults()


def process_cli():
    """
    Process the command line interface
    """
    parser = OptionParser()
    parser.add_option("-m", "--mapfile", dest="mapfile", help="Mapfile that will be used. Mandatory option")
    parser.add_option("-f", "--filetoprocess", dest="filetoprocess", help="The file that will generated from a map or used to generate a map")
    parser.add_option("-r", "--reassemble", dest="reassemble", action="store_true", help="Reassemble a file specified in the map file")
    parser.add_option("-g", "--generate", dest="generatemap", action="store_true", 
                      help="Generate a map for the file specified and either create the map file or append to it")
    parser.add_option("-x", "--examine", dest="examine", action="store_true", help="Examine a mapfile and chunks and verify them")

    (options, args) = parser.parse_args()
    if not options.mapfile:
        print "You must specify a mapfile!"
        exit(-1)

    if not options.filetoprocess:
        print "You must specify a file to process!"
        exit(-1)

    if not (options.generatemap or options.examine or options.reassemble):
        print "You must pick a thing to do... generate, examine or reassemble"
        exit(-1)

    return (options, args)


def main():
    (o, a) = process_cli()
    x = chunkomatic()
    if o.generatemap:
        x.setup_mapconfig()
        x.generate_chunks(o.filetoprocess)
        x.write_mapfile(o.mapfile)
    
    if o.examine:
        x.load_mapfile(o.mapfile)
        if not x.examine(o.filetoprocess):
            print "Validation Failed"
            exit (-1)
        


if __name__ == '__main__':
    main()
                      
