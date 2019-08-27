import csv

if __name__ == '__main__':
    fasttext_model_path = "/home/mapp/masterprojekt/named-entity-linker/wiki-news-300d-1M.vec"
    fasttext_model_linking = "/home/mapp/masterprojekt/named-entity-linker/improved_linking_fasttext.csv"
    output_fasttext_path = "all_caps_linking.csv"

    with open(fasttext_model_path, "r") as model_file:
        csv_reader = csv.reader(model_file, delimiter=" ", quoting=csv.QUOTE_NONE)
        next(csv_reader)
        tags = {row[0] for row in csv_reader}

    tag_id_pairs = []
    with open(fasttext_model_linking, "r") as linking_file:
        csv_reader = csv.reader(linking_file)
        next(csv_reader)
        tag_id_pairs = [(row[0], row[1]) for row in csv_reader]

    with open(output_fasttext_path, "w+") as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(["embedding_label", "knowledgebase_id"])
        for tag_id_pair in tag_id_pairs:
            tag = tag_id_pair[0]
            if tag.upper() in tags:
                csv_writer.writerow([tag.upper(), tag_id_pair[1]])
            else:
                csv_writer.writerow([tag, tag_id_pair[1]])

