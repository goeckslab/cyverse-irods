#!/usr/bin/env python

import argparse
from os import environ, makedirs, path, walk

from irods.exception import DoesNotExist
from irods.models import Collection, DataObject
from irods.session import iRODSSession

from decorators import target_format


class CyVerseiRODS:
    """
    A class for interaction with CyVerse via iRODS. User information is taken
    from environment variables:
        CYVERSE_IRODS_USER
        CYVERSE_IRODS_PASS

    Some points of note about iRODS nomenclature:
        - a 'COLLECTION' is analogous to a directory
        - a 'DATA OBJECT' is a file
    """

    KWARGS = {
        "host" : "data.iplantcollaborative.org",
        "port" : "1247",
        "zone" : "iplant"
    }

    ENV_INFO = {
        "CYVERSE_IRODS_USER" : "user",
        "CYVERSE_IRODS_PASS" : "password"
    }


    def sense_env(self):
        try:
            for x in self.ENV_INFO:
                self.KWARGS[self.ENV_INFO[x]] = environ[x]
        except KeyError as ke:
            print("KeyError ({}): Required argument absent".format(ke))
            raise ke

    def __init__(self, **kwargs):
        if kwargs and "user" in kwargs and "password" in kwargs:
            self.KWARGS["user"] = kwargs["user"]
            self.KWARGS["password"] = kwargs["password"]
        else:
            self.sense_env()

        self.user_dir = "/iplant/home/{}".format(self.KWARGS["user"])
        self.session = iRODSSession(**self.KWARGS)
        self.api_colls = self.session.collections
        self.api_data_objs = self.session.data_objects

    @target_format
    def get(self, target):
        # collection or data object?
        if self.api_data_objs.exists(target):
            return self.get_data_objects(target, False)
        if self.api_colls.exists(target):
            return self.get_collections(target, False)

    def disambiguate_dir(self, dir):
        if dir[0] == "~":
            dir = path.expanduser(dir)
        dir = path.abspath(dir)
        return dir

    def walker(self, file_path, disambiguate=False):
        if disambiguate:
            file_path = self.disambiguate_dir(file_path)
        ws = walk(file_path)
        dirs = []
        files = []
        for (dpath, unused, fname) in ws:
            dpath = dpath[len(file_path):]
            if dpath and dpath[0] == '/':
                dpath = dpath[1:]
            # add files to file list
            for f in fname:
                if dpath:
                    files.append(dpath + "/" + f)
                else:
                    files.append(f)
            # add to dirs
            dirs.append(dpath)
        # sort dirs
        dirs.sort()
        min_dirs = []
        for da in dirs:
            for db in dirs:
                skip = False
                if da == db:
                    continue
                if db.startswith(da):
                    skip = True
                    break
            if not skip:
                min_dirs.append(da)
            else:
                continue
        return (min_dirs, files, file_path)

    def recursive_upload(self, file_path, dest):
        file_path = self.disambiguate_dir(file_path)
        if path.isfile(file_path):
            self.file_to_data_object(file_path, dest)
        elif path.isdir(file_path):
            dir = '/' + file_path.split('/')[-1] + '/'
            dirs, files, local_path = self.walker(file_path)
            for d in dirs:
                d_dest = dest + dir + d
                self.make_collection(d_dest)
            for f in files:
                f_dest = dest + dir + f
                f_local = local_path + '/' + f
                self.file_to_data_object(f_local, f_dest)
        else:
            raise OSError("File/Directory {} not found.".format(file_path))


    def mkdir(self, dest):
        try:
            makedirs(dest)
        # python 2.7+ race condition
        except OSError:
            if not path.isdir(dest):
                raise

    def make_collection(self, dest):
        return self.api_colls.create(dest)

    def data_object_to_file(self, obj, dest):
        dest = self.disambiguate_dir(dest)
        self.mkdir(dest)
        name = obj.name
        file_dest = path.join(dest, name)
        with obj.open('r+') as src_f:
            with open(file_dest, 'wb') as dst_f:
                for line in src_f:
                    dst_f.write(line)

    def file_to_data_object(self, file_path, dest):
        if not path.isfile(file_path):
            raise OSError("File {} not found.".format(file_path))
        self.api_data_objs.put(file_path, dest)

    @target_format
    def get_collections(self, target, target_check=False):
        if target_check:
            if not self.api_data_objs.exists(target):
                return None
        return self.api_colls.get(target)

    @target_format
    def get_data_objects(self, target, target_check=False):
        if target_check:
            if not self.api_data_objs.exists(target):
                return None
        return self.api_data_objs.get(target)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="CyVerse/iRODS interaction")
    ap.add_argument("--upload", action='store_true')
    ap.add_argument("--localsource")
    ap.add_argument("--remotedestination")
    ap.add_argument("--user")
    ap.add_argument("--password")

    args = ap.parse_args()

    kwargs = {}
    if args.user and args.password:
        kwargs["user"] = args.user
        kwargs["password"] = args.password

    # initialize connection
    conn = CyVerseiRODS(**kwargs)

    # upload
    if args.upload:
        if args.localsource is None:
            parser.error("--upload requires --localsource, --remotedestination optional")
        else:
            args.remotedestination = conn.user_dir if not args.remotedestination else args.remotedestination
            conn.recursive_upload(args.localsource, args.remotedestination)
