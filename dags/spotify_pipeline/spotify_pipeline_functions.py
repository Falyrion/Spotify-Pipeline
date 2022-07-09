import datetime
import pandas as pd
import requests
import ibm_db


def request_data(auth_token) -> dict:
    """ Request and download data from spotify_pipeline

    :param auth_token: String; The token to authorize the request at the spotify_pipeline api.
    :return: Json
    """

    # Get date of day before (because we want last 24h)
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000  # As unix timestamp (miliseconds)

    # Define header for get-request
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=auth_token)
    }

    # Request data from spotify_pipeline.
    r = requests.get(
        f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
        headers=headers
    )

    # Convert to json and return
    return r.json()


def filter_data(data) -> pd.DataFrame:
    """ Extract only wanted columns from the downloaded data

    :param data: Dictionary; The raw data in json-format
    :return: pandas.DataFrame
    """

    # Extract only wanted parts from the data
    song_names = []
    artist_names = []
    timestamp = []

    for entry in data["items"]:
        song_names.append(entry["track"]["name"])
        artist_names.append(entry["track"]["album"]["artists"][0]["name"])
        timestamp.append(entry["played_at"][:19])  # -> From format "2022-06-29T11:17:09.780Z" to "2022-06-29T11:17:09"

    # Create dictionary from the lists
    songs_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "timestamp": timestamp,
    }

    # Convert to pandas dataframe
    return pd.DataFrame(songs_dict, columns=["song_name", "artist_name", "timestamp"])


def check_if_data_valid(data: pd.DataFrame) -> bool:
    """ Validate data

    :param data: pd.DataFrame; The dataframe with the songs data
    :return: Boolean
    """

    # Check if dataframe is empty
    if data.empty:
        print("No songs downloaded. Finishing execution.")
        return False

    # Check primary key
    if not pd.Series(data["timestamp"]).is_unique:
        raise Exception("Primary Key is violated")

    # Check for NULLs
    if data.isnull().values.any():
        raise Exception("NULL value found")

    # Make sure that all timestamps are from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = data["timestamp"].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp[:10], "%Y-%m-%d") != yesterday:
            raise Exception("At least one songs is not from the last 24 hours")

    return True


def save_data_db2(data: pd.DataFrame, connection_data: str,) -> bool:
    """ Save data to database

    :param data: pd.DataFrame; The data to save
    :param connection_data: String; Data to create connection
    :return: Boolean
    """
    # Connect to database
    connection = ibm_db.connect(connection_data, "", "")

    if not ibm_db.active(connection):
        print("Failed to connect to database")
        return False

    # Convert df to tuple of tuples
    df_as_tuples = tuple([tuple(x) for x in data.values])

    # Prepare statement
    statement = ibm_db.prepare(connection, "INSERT INTO song_data VALUES(?, ?, ?)")

    # Execute statement
    lines = ibm_db.execute_many(statement, df_as_tuples)
    print(f"Inserted {lines} lines to the database")

    # Close connection
    connection.close()
    return True


def spotify_pipeline():
    """ The main function of this script

    :return: None
    """
    # Declare constant variables
    # -> Token can be generated here: https://developer.spotify.com/console/get-recently-played/
    token = ""  # Enter token here

    # Fetch data
    raw_data = request_data(auth_token=token)

    # Extract wanted columns
    songs_df = filter_data(data=raw_data)

    # Validate data. If not valid stop here and do not save to database.
    if not check_if_data_valid(data=songs_df):
        return

    # Load data to database
    # Fill out missing parts
    con_string = "DATABASE=;" \
                 "HOSTNAME=;" \
                 "PORT=;" \
                 "PROTOCOL=TCPIP;" \
                 "UID=;" \
                 "PWD=;" \
                 "SECURITY=SSL"

    save_result = save_data_db2(data=songs_df, connection_data=con_string)
    if save_result:
        print("Inserted data successfully to database")
    else:
        print("Could not save data to database")
