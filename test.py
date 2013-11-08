import boto
import time
import argparse
import os
import sqlite3
import bz2

from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
import os

    
import shutil

import logging

logger = logging.getLogger("chipmunk")

ACCESS_KEY_ID       = "AKIAIUTI6QAVHWRD6CDA"
SECRET_ACCESS_KEY   = "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"
VALUT               = "TEST_VALUT_1"
TEMP_WORK_DIR       = ".chipmunk_temp"

'''
# boto.connect_glacier is a shortcut return a Layer2 instance 


vault = glacier_connection.create_vault("__test_vault_2__")

print "--" * 30
print "Vault: %s" % vault
print "--" * 30

# You must keep track of the archive_id
archive_id = vault.upload_archive("TEST_FILE")

print archive_id
'''

def walk_files_in_directory(directory_path):
    assert os.path.exits(directory_path), "Path doesn't Exist: %s " % directory_path
    for dirname, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            yield os.path.join(dirname, filename)

def zipdir(basedir, archivename):
    assert os.path.isdir(basedir)
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(basedir):
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(basedir)+len(os.sep):]
                z.write(absfn, zfn)

def bzip_file(filehandle):
    outfile = filehandle + ".bz2"
    with open(infile, 'rb') as input:
        with bz2.BZ2File(outfile, 'wb', compresslevel=9) as output:
            shutil.copyfileobj(input, output)
    return outfile

def compress_directory_for_archive(directory_path):
    '''
        Takes a directory and returns a path to a 
        compressed version of it
    '''
    logging.debug("Compressing Directory: %s", directory_path)
    assert os.path.exists(directory_path), "Directory doesn't exist: %s" % directory_path
    assert os.path.isdir(directory_path), "Path is not a directory: %s" % directory_path
    _, dirname = os.path.split(directory_path)
    logging.debug("Dirname: %s", dirname)
    final_result_path = os.path.join(TEMP_WORK_DIR, dirname + ".zip")
    logging.debug("Destination path: %s", dest_path)
    zipdir(directory_path, final_result_path)
    return final_result_path

def archive_directory(directory_to_archive):
    logger.debug("Archiving Directory: %s", directory_to_archive)
    compressed_path = compress_directory_for_archive(directory_to_archive)
    logging.debug("Compressed Directory: %s", compressed_path)

def configure():
    parser = argparse.ArgumentParser(description='Upload a directory to glacier')
    parser.add_argument('--directory', type=str, help='directory to upload')

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    shutil.rmtree(TEMP_WORK_DIR)
    os.mkdir(TEMP_WORK_DIR)
    
    glacier_connection = boto.connect_glacier(
      aws_access_key_id     = ACCESS_KEY_ID,
      aws_secret_access_key = SECRET_ACCESS_KEY,
    )
    return parser.parse_args()

def connect_to_glacier(aws_access_key, awe_secret):
    return 

def main():
    args = configure()

    directory_to_archive = args.directory

    archive_directory(directory_to_archive)

    # shutil.rmtree(TEMP_WORK_DIR)

if __name__ == "__main__":
    main()
