import csv
import mysql.connector

def missing_in_cache():
    source_set = set()
    cache_set = set()
    with open("living_people_wikipedia_page_id.csv") as source_file, open("living_people_cache.csv") as cache_file:
        source_reader = csv.reader(source_file)
        next(source_reader)
        cache_reader = csv.reader(cache_file)
        next(cache_reader)

        for row in source_reader:
            source_set.add(row[0])

        for row in cache_reader:
            cache_set.add(row[0])

    diff = source_set.difference(cache_set);

    for i in diff:
        print(i)

def list_incorrectly_linked_entities():
    connection = mysql.connector.connect(host='localhost',
                             user='root',
                             password='toor',
                             database='mpss2019',
                                         use_pure=True)

    cursor = connection.cursor(prepared=True)
    #query = "select pp_value from page_props where pp_propname='wikibase_item' and pp_page = %s and pp_value = %s;"
    query = "select pp_value, title " \
            "from page_props pp, living_people lv "\
            "where pp.pp_propname='wikibase_item' and pp_page = lv.page_id"

    with open("living_people_wikipedia_page_id.csv") as source_file, open("living_people_linking.csv") as linking_file:
        source_reader = csv.reader(source_file)
        next(source_reader)
        linking_reader = csv.reader(linking_file)
        next(linking_reader)
        linking = dict()

        cursor.execute(query)
        records = cursor.fetchall()
        true_dict = dict()

        for record in records:
            true_dict[record[0]] = record[1]

        for row in linking_reader:
            wikidata_id = row[1]
            entity_name = row[0]
            name_in_db = true_dict.get(wikidata_id, None)
            if name_in_db is None:
                print(f"{wikidata_id} not in db")
            elif name_in_db != entity_name:
                print(f"{wikidata_id} linked to {entity_name} instead of {name_in_db}")



        #
        # i = 0
        # for row in source_reader:
        #     i = i + 1;
        #     if i%1000 == 0:
        #         print(i)
        #     name = row[0]
        #     wikipedia_page_id = int(row[1])
        #     wikidata_id = linking.get(name, None)
        #     if wikidata_id is None:
        #         #print(f"{name} not found.")
        #         pass
        #     else:
        #         cursor.execute(query, (wikipedia_page_id, wikidata_id))
        #         cursor.fetchone()
        #         if cursor.rowcount <= 0:
        #             print(f"{name}({wikipedia_page_id}) not found.")


if __name__ == '__main__':
    list_incorrectly_linked_entities()
