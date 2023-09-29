import os
from time import perf_counter

from unidecode import unidecode
from gutenbergdammit.ziputils import searchandretrieve
import evadb


def download_text():
    parsed_file_path = "new_gut.txt"

    # Process file each paragraph per line.
    new_f = open(parsed_file_path, "w")
    merge_line = ""

    for info, text in searchandretrieve("gutenberg-dammit-files-v002.zip", {'Title': 'Study'}):
        for line in text:
            if line == "\n":
                new_f.write(f"{merge_line}\n")
                merge_line = ""
            else:
                merge_line += line.rstrip("\n")

    return parsed_file_path


def read_text_line(path, num_token=1000):
    # For simplicity, we only keep letters.
    whitelist = set(".!?abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    with open(path, "r") as f:
        line_itr = 0
        for line in f.readlines():
            line_itr = line_itr + 1
            if line_itr % 100000 == 0:
                print("line: " + str(line_itr))
            for i in range(0, len(line), num_token):
                cut_line = line[i : min(i + 1000, len(line))]
                cut_line = "".join(filter(whitelist.__contains__, cut_line))
                yield cut_line

            if line_itr == 1000000:
                break


def try_execute(conn, query):
    try:
        conn.query(query).execute()
    except Exception:
        pass


def create_index(story_path: str):
    path = os.path.dirname(os.getcwd())
    cursor = evadb.connect().cursor()

    story_table = "TablePPText"
    story_feat_table = "FeatTablePPText"
    index_table = "IndexTable"

    timestamps = {}
    t_i = 0

    timestamps[t_i] = perf_counter()
    print("Setup Function")

    Text_feat_function_query = f"""CREATE FUNCTION IF NOT EXISTS SentenceFeatureExtractor
            IMPL  './sentence_feature_extractor.py';
            """

    cursor.query("DROP FUNCTION IF EXISTS SentenceFeatureExtractor;").execute()
    cursor.query(Text_feat_function_query).execute()

    cursor.query(
        f"SELECT * FROM {story_table};"
    ).execute()

    cursor.query(f"DROP TABLE IF EXISTS {story_table};").execute()
    cursor.query(f"DROP TABLE IF EXISTS {story_feat_table};").execute()

    t_i = t_i + 1
    timestamps[t_i] = perf_counter()
    print(f"Time: {(timestamps[t_i] - timestamps[t_i - 1]) * 1000:.3f} ms")

    print("Create table")

    cursor.query(f"CREATE TABLE {story_table} (id INTEGER, data TEXT(1000));").execute()

    # Insert text chunk by chunk.
    for i, text in enumerate(read_text_line(story_path)):
        print("text: --" + text + "--")
        ascii_text = unidecode(text)
        cursor.query(
            f"""INSERT INTO {story_table} (id, data)
                VALUES ({i}, '{ascii_text}');"""
        ).execute()

    t_i = t_i + 1
    timestamps[t_i] = perf_counter()
    print(f"Time: {(timestamps[t_i] - timestamps[t_i - 1]) * 1000:.3f} ms")

    print(cursor.query(
        f"SELECT * FROM {story_table};"
    ).execute())

    print("Extract features")

    # Extract features from text.
    cursor.query(
        f"""CREATE TABLE {story_feat_table} AS
        SELECT SentenceFeatureExtractor(data), data FROM {story_table};"""
    ).execute()

    t_i = t_i + 1
    timestamps[t_i] = perf_counter()
    print(f"Time: {(timestamps[t_i] - timestamps[t_i - 1]) * 1000:.3f} ms")

    print(cursor.query(
        f"SELECT * FROM {story_feat_table};"
    ).execute())

    print("Create index")

    # Create search index on extracted features.
    cursor.query(
        f"CREATE INDEX {index_table} ON {story_feat_table} (features) USING QDRANT;"
    ).execute()

    t_i = t_i + 1
    timestamps[t_i] = perf_counter()
    print(f"Time: {(timestamps[t_i] - timestamps[t_i - 1]) * 1000:.3f} ms")

    print(f"Total Time: {(timestamps[t_i] - timestamps[0]) * 1000:.3f} ms")


def main():
    story_path = download_text()
    create_index(story_path)


if __name__ == "__main__":
    main()