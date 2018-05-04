from os import environ


from decorators import target_format
from irods.exception import DoesNotExist
from irods.models import Collection, DataObject
from irods.session import iRODSSession

#print("KW ARGS: {}".format(KWARGS))

#with iRODSSession(**KWARGS) as session:
    #query = session.query(Collection.name, DataObject.name)
    #print(session.collections.exists("/iplant/home/sargentl"))
    # obj = session.data_objects.get("/iplant/home/sargentl")
    #print("guessing this doesnt happen")
    #for result in query:
    #    print('{}/{} id={} size={}'.format(result[Collection.name], result[DataObject.name], result[DataObject.id], result[DataObject.size]))


class CyVerseiRODS():
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

    def __init__(self):
        try:
            for x in self.ENV_INFO:
                self.KWARGS[self.ENV_INFO[x]] = environ[x]
            self.user_dir = "/iplant/home/{}".format(self.KWARGS["user"])
        except KeyError as ke:
            print("KeyError ({}): Required argument absent from environment".format(ke))
            raise ke
        #print("KWARGLES {}".format(self.KWARGS))
        self.session = iRODSSession(**self.KWARGS)
        self.collections = self.session.collections
        self.data_objects = self.session.data_objects

    @target_format
    def get_collections(self, target):
        colls = self.session.collections.get(target)
        return colls

    @target_format
    def get_data_objects(self, target):
        pass

    @target_format
    def ls(self, target):
        # determine if target is data_object or collection
        if self.data_objects.exists(target):
            return "[Data Object] {}".format(target)
        elif not self.collections.exists(target):
            raise DoesNotExist("target \"{}\" does not exist".format(target))
        # is a directory, so we can inspect
        coll = self.get_collections(target=target)
        for c in coll.subcollections:
            print(c)
        for d in coll.data_objects:
            print(d)
