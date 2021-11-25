import json
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
df = pd.read_json('/home/bashar/Downloads/input.json')
# print(df)
# normalising input dataset
json_struct = json.loads(df.to_json(orient="records"))
# print(json_struct)
df = pd.json_normalize(json_struct, record_path=['songs'], meta=['record', 'artist'])
# print(df)

# # expanding record json object into data frame columns

record_columns_list = list(df['record'])
# print(record_columns_list)

records = pd.json_normalize(record_columns_list)
# print(records)

artist_columns_list = list(df['artist'])
# print(artist_columns_list)

artists = pd.json_normalize(artist_columns_list)
# print(artists)

# Converting Into DataFrame

record_sub_dframe = pd.DataFrame(records)
# print(record_sub_dframe)


artist_sub_dframe = pd.DataFrame(artists)
# print(artist_sub_dframe)

record_sub_dframe['release_date'] = pd.to_datetime(record_sub_dframe['release_date'])
# print(record_sub_dframe['release_date'])
record_sub_dframe['release_date'] = record_sub_dframe['release_date'].apply(lambda x: x.timestamp() if not pd.isna(x) else pd.NA).astype('int64')
# print(record_sub_dframe)

# Renaming columns name
new_df = record_sub_dframe.rename(columns={"name": "recordName", "genre": "recordGenre", "release_date": "recordReleaseDate"})
# print(new_df)

artist_col = artist_sub_dframe.rename(columns={"name": "artistName"})
# print(artist_col)
# Joining DataFrames
result = pd.concat([new_df, artist_col], axis=1, join='inner')
# print(result)

song = df[["name", "duration"]]
song_df = pd.DataFrame(song)

def get_sec(duration):
    """Get Seconds from time."""
    m, s = duration.split(':')
    return int(m) * 60 + int(s)

song_df["duration"] = song_df["duration"].apply(get_sec).astype('int64')

# print(song_df)


song_col = song_df.rename(columns={"name": "songName", "duration": "songDuration"})
# print(song_col)
song_data = pd.concat([result, song_col], axis=1, join='inner')
# print(song_data)



# Filter out records with genre that is not specified in output schema -- ROCK, DANCE, POP

filtered_df = song_data[song_data["recordGenre"] != "SOUNDTRACK"]
# print(filtered_df)

# grouped_df =
grouped = filtered_df.groupby("recordName")
aggregate_df = grouped.agg({"songName" : "size", "songDuration" : "sum"})
# print(aggregate_df)
aggregate_df = aggregate_df.rename(columns={"songName" : "song_count", "songDuration" : "sum_of_songDuration"})
# print(aggregate_df)
expected_df = filtered_df.merge(aggregate_df, how = 'inner', on = ["recordName"])
# print(expected_df)

# Determine record type
# SINGLE - Only 1 song
# EP - More than 1 song & Total song duration of record is less than 30 minutes
# ALBUM - More than 1 song & Total song duration of record is equal or more than 30 minutes

def record_type(song_count, sum_of_songDuration):
    if song_count == 1:
        return "SINGLE"
    elif song_count > 1 and sum_of_songDuration < 1800:
        return "EP"
    elif song_count > 1 and sum_of_songDuration >= 1800:
        return "ALBUM"
    else:
        return "No clue"

expected_df["recordType"] = expected_df.apply(lambda x: record_type(x["song_count"], x["sum_of_songDuration"] ), axis=1)
# print(expected_df)
expected_output =  expected_df[["songName", "songDuration", "recordName", "recordGenre", "recordType", "recordReleaseDate", "artistName"]]

# export to JSON
print(expected_output.to_json('/home/bashar/Downloads/outputfile.json',orient='records'))

# from dateutil.parser import parse

# print(parse("2021-03-01").timestamp())
# import datetime
# print(datetime.datetime.now().timestamp())
