import mysql.connector
from rets.http import RetsHttpClient
from tqdm import tqdm

def replace_characters(string_rep):
    rem = ["[", "]", "'"]
    for char in rem:
        if char in string_rep:
            string_rep = string_rep.replace(char, "")
    return string_rep


def store_property(remote_property, database):
    prop_dict = dict(remote_property.data[0])
    prop_search_address = "{} {} {}".format(prop_dict['StreetNumber'], prop_dict['StreetName'], prop_dict['StreetSuffix'])
    prop_dict['property_search_address'] = prop_search_address
    query = ("INSERT  INTO db_mls_resi_properties (" + ",".join(prop_dict.keys()) + ") VALUES ( " + (
            "%s, " * (len(prop_dict.values()) - 1)) + "%s)")
    cursor = database.cursor()
    cursor.execute(query, list(prop_dict.values()))
    database.commit()
    return prop_dict['ListingRid']


def fetch_remote_mlnumbers(rets_http):
    print("Fetching remote mlnumbers...")
    remote_mlnumbers = []
    remote_ml_dicts = rets_http.search(
        resource="Property",
        class_="RESI",
        query="(MLNumber=0+),~(MLNumber=.EMPTY.),(Status=|A,P)",
        select="MLNumber"
    )
    for mlnumber_dict in remote_ml_dicts.data:
        remote_mlnumbers.append(mlnumber_dict["MLNumber"])
    return set(remote_mlnumbers)


def fetch_local_mlnumbers(database):
    local_mlnumbers = []
    print("Creating local property list...")
    db_cursor = database.cursor()
    query = "SELECT MLNumber FROM db_mls_resi_properties;"
    db_cursor.execute(query)
    for MLNumber in db_cursor:
        local_mlnumbers.extend(list(MLNumber))
    return set(local_mlnumbers)


def delete_old_properties(old_properties, database):
    if len(old_properties) > 0:
        print("Purging {} old properties...".format(len(old_properties)))
        i = 0
        cursor = database.cursor()
        query = ('DELETE FROM db_mls_resi_properties WHERE MLNumber IN(' + ('%s, ' * (len(old_properties) - 1)) + '%s)')
        cursor.execute(query, tuple(old_properties))
        database.commit()
        print("Old properties purged.")
    else:
        print("No properties to purge...")


def add_new_properties(new_properties, rets_client, database):
    if len(new_properties) > 0:
        print("Importing {} new properties...".format(len(new_properties)))
        pbar = tqdm(total=len(new_properties))
        for mlnumber in new_properties:
            remote_property = rets_client.search(
                resource='Property',
                class_='RESI',
                query='(MLNumber={}),~(MLNumber=.EMPTY.),(Status=|A,P)'.format(mlnumber),
            )
            listing_rid = store_property(remote_property, database)
            photos = rets_client.get_object(
                resource='Property',
                object_type='Photo',
                resource_keys=listing_rid,
            )
            """
            TODO: Store photos from rets client to database for new properties
            Currently there is an issue with rets-python returning an empty tuple for the photos
            Working on solution
            """
            pbar.update(1)
        print("New properties imported...")
        pbar.close()
        del pbar
    else:
        print("No new properties to import...")


def update_cities(database, database_cursor):
    query = "SELECT city FROM db_mls_property_cities"
    database_cursor.execute(query)
    current_cities = []
    for city in database_cursor:
        current_cities.extend(list(city))

    query = "SELECT city FROM db_mls_resi_properties"
    database_cursor.execute(query)
    new_cities = []
    for city in database_cursor:
        new_cities.extend(list(city))

    del_cities = list(set(current_cities).difference(set(new_cities)))
    if len(del_cities) > 0:
        query = "DELETE FROM db_mls_property_cities WHERE city IN(" + ("%s, " * (len(del_cities) - 1)) + "%s)"
        database_cursor.execute(query, del_cities)
        database.commit()
        print("{} cities purged.".format(len(del_cities)))
    else:
        print("No cities to purge.")

    import_cities = list(set(new_cities).difference(set(current_cities)))
    if len(import_cities) > 0:
        query = ("INSERT INTO db_mls_property_cities (city) VALUES " + ("(%s)," * (len(import_cities) - 1)) + "(%s)")
        database_cursor.execute(query, import_cities)
        database.commit()
        print("{} cities imported.".format(len(import_cities)))
    else:
        print("No cities to import.")


def update_counties(database, database_cursor):
    query = "SELECT county FROM db_mls_property_counties"
    database_cursor.execute(query)
    current_counties = []
    for county in database_cursor:
        current_counties.extend(list(county))

    query = "SELECT county from db_mls_resi_properties"
    database_cursor.execute(query)
    new_counties = []
    for county in database_cursor:
        new_counties.extend(list(county))
    del_counties = list(set(current_counties).difference(set(new_counties)))

    if len(del_counties) > 0:
        query = "DELETE FROM db_mls_property_counties WHERE county IN(" + ("%s, " * (len(del_counties) - 1)) + "%s)"
        database_cursor.execute(query, del_counties)
        database.commit()
        print("{} counties purged.".format(len(del_counties)))
    else:
        print("No counties to purge.")

    import_counties = list(set(new_counties).difference(set(current_counties)))
    if len(import_counties) > 0:
        query = "INSERT  INTO db_mls_property_counties (county) VALUES " + "(%s)," * (len(import_counties) - 1) + "(%s)"
        database_cursor.execute(query, import_counties)
        database.commit()
        print("{} counties imported.".format(len(import_counties)))
    else:
        print("No counties to import.")


def update_cities_and_counties(database):
    print('Updating cities and counties...')
    cursor = database.cursor()
    update_cities(database, cursor)
    update_counties(database, cursor)
    database.commit()


# MySQL Connection
db = mysql.connector.connect(
    user="",
    password="",
    host="",
    database=""
)

# Rets Connection
client = RetsHttpClient(
    login_url="",
    username="",
    password="",
    user_agent="",
    rets_version=""
)
client.login()

# Store remote properties to database
local_mlnumbers = fetch_local_mlnumbers(db)
remote_mlnumbers = fetch_remote_mlnumbers(client)

remove_local = local_mlnumbers.difference(remote_mlnumbers)
add_remote = remote_mlnumbers.difference(local_mlnumbers)

delete_old_properties(remove_local, db)
add_new_properties(add_remote, client, db)
update_cities_and_counties(db)  # for the moment this is separate from the delete/add functions
