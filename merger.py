import csv

if __name__ == '__main__':
    fasttext_path = "wiki-news-300d-1M-linking.csv"
    glove_path = "glove-linking.csv"
    # output_fasttext_path = "fasttext-glove-linking.csv"
    output_fasttext_path = "fasttext-glove-id-intersect-for-fasttext.csv"
    output_glove_path = "fasttext-glove-id-intersect-for-glove.csv"

    # dictionary QID, Tags
    with open(fasttext_path, "r") as fasttext_file:
        reader = csv.reader(fasttext_file)
        next(reader)
        fasttext_dict = dict()
        for row in reader:
            tag = row[0]
            id = row[1]
            tags = fasttext_dict.get(id, None)
            if not tags:
                tags = []
                fasttext_dict[id] = tags
            tags.append(tag)

    with open(glove_path, "r") as glove_file:
        reader = csv.reader(glove_file)
        next(reader)
        glove_dict = dict()
        for row in reader:
            tag = row[0]
            id = row[1]
            tags = glove_dict.get(id, None)
            if not tags:
                tags = []
                glove_dict[id] = tags
            tags.append(tag)

    intersecting_ids = set(fasttext_dict.keys()) & set(glove_dict.keys())

    with open(output_fasttext_path, "w+") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["embedding_label", "knowledgebase_id"])
        for id in intersecting_ids:
            for tag in fasttext_dict[id]:
                writer.writerow([tag, id])

    with open(output_glove_path, "w+") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["embedding_label", "knowledgebase_id"])
        for id in intersecting_ids:
            for tag in glove_dict[id]:
                writer.writerow([tag, id])
