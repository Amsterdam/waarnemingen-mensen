from django.conf import settings
from objectstore import objectstore


def get_objstore_directory_meta(directory):
    '''
    Retrieve directory meta data from the object store. The meta data contains
    a list of all sub-directories/files. directory param is the path.
    '''
    conn = objectstore.get_connection(settings.OBJECTSTORE_CONF)
    return objectstore.get_full_container_list(conn, directory)


def get_objstore_file(name, directory):
    '''
    Fetch and return file from object store.

    Parameters:
    name (str): name of the file to be fetched.
    directory (str): Path of the directory the file is in.
    '''
    conn = objectstore.get_connection(settings.OBJECTSTORE_CONF)
    return conn.get_object(directory, name)
