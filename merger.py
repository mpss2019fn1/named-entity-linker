import csv

if __name__ == '__main__':
    fasttext_path = "wiki-news-300d-1M-linking.csv"
    glove_path = "glove-linking.csv"
    # output_fasttext_path = "fasttext-glove-linking.csv"
    output_fasttext_path = "fasttext-glove-linking-for-fasttext.csv"
    output_glove_path = "fasttext-glove-linking-for-glove.csv"

    with open(fasttext_path, "r") as fasttext_file:
        reader = csv.reader(fasttext_file)
        next(reader)
        fasttext_dict = {row[0].lower(): (row[0], row[1]) for row in reader}

    with open(glove_path, "r") as glove_file:
        reader = csv.reader(glove_file)
        next(reader)
        glove_dict = {row[0].lower(): (row[0], row[1]) for row in reader}

    intersecting_entities = set(fasttext_dict.keys()) & set(glove_dict.keys())

    with open(output_fasttext_path, "w+") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["embedding_label", "knowledgebase_id"])
        for entity in intersecting_entities:
            writer.writerow([fasttext_dict[entity][0], fasttext_dict[entity][1]])

    with open(output_glove_path, "w+") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["embedding_label", "knowledgebase_id"])
        for entity in intersecting_entities:
            writer.writerow([glove_dict[entity][0], fasttext_dict[entity][1]])
