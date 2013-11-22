from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
import argparse
import boto
import bz2
import datetime
import logging
import os
import os
import shutil
import sqlite3
import time

logger = logging.getLogger("chipmunk")

ACCESS_KEY_ID       = "AKIAIUTI6QAVHWRD6CDA"
SECRET_ACCESS_KEY   = "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"
VALUT               = "TEST_VALUT_1"

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
    assert os.path.exists(directory_path), \
        "Path doesn't Exist: %s " % directory_path
    for dirname, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            yield os.path.join(dirname, filename)

def put_file_in_glacier(filehandle, vault):
    archive_id = vault.upload_archive(filehandle)
    return archive_id

def archive_glacier(filehandle, vault, db_conn, directory, tag):
    archive_id  = put_file_in_glacier(filehandle, vault)
    _, filename = os.path.split(filehandle)
    db_conn.execute("""
        INSERT INTO archived_files VALUES (?, ?, ?, ?, ?, ?);
    """, [
        filename,
        filehandle,
        tag,
        archive_id,
        vault.name,
        datetime.datetime.utcnow(),
    ])
    db_conn.commit()

def archive_directory(db_conn, vault, directory, tag):
    logger.debug("Archiving Directory: %s", directory)
    for file_handle in walk_files_in_directory(directory):
        archive_glacier(file_handle, vault, db_conn, directory, tag)

def configure():
    parser = argparse.ArgumentParser(description='Upload a directory to glacier')
    parser.add_argument('--directory', type=str, help='directory to upload')
    parser.add_argument('--tag', type=str, help='tag the files')
    parser.add_argument('--db', type=str, help='location of sqlite3 db')
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    return parser.parse_args()

def connect(db):
    glacier_conn = boto.connect_glacier(
      aws_access_key_id     = ACCESS_KEY_ID,
      aws_secret_access_key = SECRET_ACCESS_KEY,
    )

    db_conn = sqlite3.connect(db)
    return db_conn, glacier_conn

def prep_db(db_conn):
    # Create table if it doesn't exist
    db_conn.execute('''
        CREATE TABLE IF NOT EXISTS archived_files
        (name text, path text, tag text, archive_id text, vault text, archived_dt datetime);
    ''')

def main():
    args = configure()

    directory = args.directory
    tag       = args.tag
    sqlitedb  = args.db

    db_conn, glacier_conn = connect(sqlitedb)
    prep_db(db_conn)

    vault = glacier_conn.create_vault(VALUT)

    archive_directory(db_conn, vault, directory, tag)

if __name__ == "__main__":
    main()
