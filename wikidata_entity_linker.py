import sys

import requests
import csv
import math
import os
import argparse

from named_entity_linker import NamedEntityLinker, NamedEntity, NamedEntityLinking


class WikidataNamedEntity(NamedEntity):

    def __init__(self, entity, linked_entity, description):
        NamedEntity.__init__(self, entity, linked_entity)
        self.description = description

    def __str__(self):
        return '{}, {}, {}'.format(self.entity, self.linked_entity, self.description)


class PersistentEntityLinker(NamedEntityLinker):

    def __init__(self, filename):
        """

        :param filename: Path to a csv file which will be used to store entity linkings.
        """
        self._filename = filename
        self._initialize_dictionary()
        self._dictionary_file = open(filename, mode='a', buffering=1)

    def _initialize_dictionary(self):
        self._dictionary = dict()

        try:
            with open(self._filename, "r") as file:
                reader = csv.reader(file, delimiter=',')
                next(reader)
                for row in reader:
                    self._dictionary[row[0]] = WikidataNamedEntity(row[0], row[1], row[2])

        except FileNotFoundError:
            with open(self._filename, "a") as file:
                csv.writer(file).writerow(["entity", "linked_entity", "description"])

    def __del__(self):
        self._dictionary_file.close()

    def persist_entity(self, wikidata_named_entity):
        writer = csv.writer(self._dictionary_file, delimiter=',')
        if os.stat(self._filename).st_size == 0:
            writer.writerow(["entity", "linked_entity", "description"])

        writer.writerow([wikidata_named_entity.entity, wikidata_named_entity.linked_entity,
                         wikidata_named_entity.description])

        self._dictionary[wikidata_named_entity.entity] = wikidata_named_entity

    def entity_id(self, entity):
        named_entity = self._dictionary.get(entity, None)
        linking_info = NamedEntityLinking.SUCCESS
        if named_entity is not None:
            if named_entity.linked_entity == '':
                named_entity = None
                linking_info = NamedEntityLinking.NO_LINKING_FOUND
        else:
            linking_info = NamedEntityLinking.NOT_FOUND
        return named_entity, linking_info

    def entity_ids(self, entities, not_found_entities=None):
        dictionary = dict()
        for entity in entities:
            named_entity, linking_info = self.entity_id(entity)

            if linking_info == NamedEntityLinking.SUCCESS:
                dictionary[entity] = named_entity
            elif not_found_entities is not None:
                not_found_entities.add(entity)

        return dictionary


class WikidataEntityLinker(NamedEntityLinker):

    def __init__(self, session=requests.Session(), entities_per_request=50):
        self._session = session
        self.entities_per_request = entities_per_request

    def _link_entities(self, entities, not_found_entities):
        """
        Requests wikidata ids to every entity in entities.
        The linking is performed using wikidata's API call to 'wbgetentities'.
        Entities which can not be linked exactly (link to a disambiguation page) will be ignored and added to
        not_found_entities.

        :param entities: Collection of entities (strings). Each entity must contain at least one character.
            WikidataEntityLinker will try to link several entities at once (in blocks of max. 50 entities per query).
            When fetching several ids at once, normalization (for example converting a word to ist base form) is not
            possible. This may result in less entries being found.
        :param not_found_entities: Expects a set which will be used to store entities that could not be linked.
        :return: Returns Dictionary<entity, WikidataNamedEntity>. Entities, which could not be linked, will not be added
            to the dictionary.
        """
        if len(entities) > self.entities_per_request:
            raise Exception(f"Only {self.entities_per_request} entities are allowed per request. You are trying to "
                            f"request {len(entities)} entities at once.")

        linked_entities = dict()
        if len(entities) == 0:
            return linked_entities

        _not_found_entities = set()
        entity_index = 0
        titles = "|".join(entities)
        normalize = True if len(entities) == 1 else False

        query_result = self._execute_query(titles, normalize)
        if query_result.status_code != 200:
            raise Exception("http request to fetch wikidata ids to entity failed")

        # parse results
        query_result_json = query_result.json()

        if 'entities' not in query_result_json:
            # add all to not found entities with warning
            raise Exception(f"http request failed, Key 'entities' not found in result. titles: {titles}, query_result: "
                            f"{query_result_json}")

        for key, value in query_result_json['entities'].items():
            description = None
            if key[0] != 'Q':
                _not_found_entities.add(value['title'])
                continue
            try:
                description = value['descriptions']['en']['value']
            except KeyError:
                pass

            while entities[entity_index] in _not_found_entities:
                entity_index += 1

            if description is not None and 'disambiguation page' in description:
                _not_found_entities.add(entities[entity_index])
            else:
                linked_entity = WikidataNamedEntity(entities[entity_index], key, description)
                linked_entities[linked_entity.entity] = linked_entity

            entity_index += 1

        if not_found_entities is not None:
            not_found_entities.update(_not_found_entities)

        return linked_entities

    def _execute_query(self, titles, normalize):
        wikidata_api_url = "https://www.wikidata.org/w/api.php"
        params = {
            'action': "wbgetentities",
            'sites': "enwiki",
            'titles': titles,
            'redirects': "yes",
            'props': 'info|descriptions',
            'format': "json",
            'languages': 'en'
        }
        if normalize:
            params['normalize'] = '1'

        return self._session.get(url=wikidata_api_url, params=params)

    def entity_id(self, entity):
        """
        Tries to link a single entity
        :param session:
        :param entity:
        :return: Dictionary<entity, WikidataNamedEntity>; Dictionary is empty if entity could not be linked.
        """
        if not entity:
            raise Exception("entity must not be None or empty.")

        not_found_entities = set()

        result = self._link_entities([entity], not_found_entities)
        assert len(result) < 2, "An entity should only be linked to max one id."

        if len(result) == 0:
            return None, NamedEntityLinking.NOT_FOUND

        return list(result.values())[0], NamedEntityLinking.SUCCESS

    def entity_ids(self, entities, not_found_entities=None):
        """
        Requests wikidata ids to every entity in entities.
        The linking is performed using wikidata's API call to 'wbgetentities'.
        The linking is performed in two iterations. In the first iteration it will be tried to link all entities without
        normalization. In the second iteration a request is send per not found entity with normalization enabled.

        :param entities: Collection of entities (strings). Each entity must contain at least one character.
            WikidataEntityLinker will try to link several entities at once (in blocks of max. 50 entities per query).
            When fetching several ids at once, normalization (for example converting a word to ist base form) is not
            possible. This may result in less entries being found.
        :param not_found_entities: Expects a list wich will be used to store entities which could not be linked.
        :return: Returns Dictionary<entity, WikidataNamedEntity>. Entities, which could not be linked, will not be added
            to the dictionary.
        """

        if len(entities) > self.entities_per_request:
            raise Exception(f"Only {self.entities_per_request} entities are allowed per request. You are trying to "
                            f"request {len(entities)} entities at once.")

        missing_batch_entities = set()
        linked_entities = self._link_entities(entities, missing_batch_entities)

        entity_counter = 0
        for entity in missing_batch_entities:
            entity_counter += 1
            linked_entity, linking_info = self.entity_id(entity)
            if linked_entity is None:
                if not_found_entities is not None:
                    not_found_entities.add(entity)
                continue

            linked_entities[entity] = linked_entity

        return linked_entities

    @staticmethod
    def normalize(entity):
        return entity.capitalize()


class WikidataEntityLinkerProxy(NamedEntityLinker):
    def __init__(self, filename, entities_per_request=50):
        """

        :param filename: Path to persistent storage
        """
        self._session = requests.Session()
        self._wikidata_entity_linker = WikidataEntityLinker(session=self._session,
                                                            entities_per_request=entities_per_request)
        self._persistent_entity_linker = PersistentEntityLinker(filename)


    def entity_id(self, entity):
        linked_entity, linking_info = self._persistent_entity_linker.entity_id(entity)

        if linking_info == NamedEntityLinking.SUCCESS:
            return linked_entity, linking_info

        if linking_info == NamedEntityLinking.NOT_FOUND:
            linked_entity, linking_info = self._wikidata_entity_linker.entity_id(entity, self._session)
            if linking_info == NamedEntityLinking.SUCCESS:
                self._persistent_entity_linker.persist_entity(linked_entity)
            else:
                self._persistent_entity_linker.persist_entity(WikidataNamedEntity(entity, "", ""))

        linked_entity, linking_info

    def entity_ids(self, entities, not_found_entities=None):
        # sammel alle entities, die wir so nicht an

        not_matched_entities = set()
        persistent_linked_entities = self._persistent_entity_linker.entity_ids(entities, not_matched_entities)

        not_cached_entities = []

        for entity in not_matched_entities:
            linked_entity, linking_info = self._persistent_entity_linker.entity_id(entity)
            if linking_info == NamedEntityLinking.NOT_FOUND:
                not_cached_entities.append(entity)
            elif not_found_entities is not None and linking_info == NamedEntityLinking.NO_LINKING_FOUND:
                not_found_entities.add(entity)

        not_matched_entities_using_wikidata = set()
        linked_entities = self._wikidata_entity_linker.entity_ids(not_cached_entities,
                                                                  not_matched_entities_using_wikidata)

        for key, entity in linked_entities.items():
            self._persistent_entity_linker.persist_entity(entity)

        for entity in not_matched_entities_using_wikidata:
            self._persistent_entity_linker.persist_entity(WikidataNamedEntity(entity, "", ""))
            if not_found_entities is not None:
                not_found_entities.add(entity)

        smaller_dict = persistent_linked_entities
        bigger_dict = linked_entities

        if len(smaller_dict) > len(bigger_dict):
            smaller_dict = linked_entities
            bigger_dict = persistent_linked_entities

        for key, value in smaller_dict.items():
            bigger_dict[key] = value

        return bigger_dict





def link_entities(entities, output_file_writer, cache, not_found_entities_file_writer):
    not_found_entities = set()
    proxy = WikidataEntityLinkerProxy(cache)
    linked_entities = proxy.entity_ids(entities, not_found_entities)

    for entity in linked_entities.values():
        output_file_writer.writerow([entity.entity, entity.linked_entity])

    for item in not_found_entities:
        not_found_entities_file_writer.writerow([item])



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Named entity linker (without context). Links words to wikidata ids.')

    parser.add_argument('model', help="File to model. This file has to be a csv file with the words to be linked "
                                      "being in the first column. The file must contain a header row.")
    parser.add_argument('-o', '--output', help="csv file to which the linking will be saved. (default='linking.csv')",
                        default="linking.csv")
    parser.add_argument('-c', '--cache', help="csv file which will be used to store all data fetched from wikidata to "
                                              "speedup future queries. (default='cache.csv')", default="cache.csv")
    parser.add_argument('-n', '--not-found-entities', help="file to which all not found entities will be stored  ("
                                                           "default='not_found_entities.txt')",
                        default="not_found_entities.txt")
    parser.add_argument('-d', '--delimiter', help="delimiter used to parse file containing the model/word list. You "
                                                  "may need to surround the delimiter with ''  ( "
                                                  "default=' ')",
                        default=" ")

    args_dict = vars(parser.parse_args())

    print(args_dict)

    model_filename = args_dict['model']
    output_filename = args_dict['output']
    cache = args_dict['cache']
    not_found_entities_filename = args_dict['not_found_entities']
    delimiter = args_dict['delimiter']

    # load model
    entities_per_request = 50

    print(f'Starting to process model file {model_filename}...')

    with open(model_filename, "r") as model_file,\
            open(output_filename, "w+") as output_file,\
            open(not_found_entities_filename, "w+") as not_found_entities_file:

        reader = csv.reader(model_file, delimiter=delimiter, quoting=csv.QUOTE_NONE)
        next(reader)

        output_file_writer = csv.writer(output_file, delimiter=',')
        output_file_writer.writerow(['embedding_label', 'knowledgebase_id'])

        not_found_entities_file_writer = csv.writer(not_found_entities_file, delimiter=',')

        entities = []
        rows_read = 0

        for row in reader:
            if '&' not in row[0] and '|' not in row[0]:
                entities.append(row[0])
                rows_read += 1
            if rows_read % entities_per_request == 0:
                link_entities(entities, output_file_writer, cache, not_found_entities_file_writer)
                entities.clear()
                print(f"{rows_read} entities processed")

        modulo = rows_read % entities_per_request
        if modulo > 0:
            link_entities(entities, output_file_writer, cache, not_found_entities_file_writer)
            print(f"{rows_read} entities processed")


    print()
    print("All done!")


    # print(f'Requesting wikidata ids  {entity_counter}/{len(missing_batch_entities)}')
    # for i in range(0, len(entities), self.entities_per_request):
    #     # split entities in batches of max entities_per_request per request.
    #     index_range = slice(i, min(len(entities), i + self.entities_per_request))
    #
    #     missing_batch_entities = set()
    #     batch_mapping = self._link_entities(entities, missing_batch_entities)
    #
    #     if self.linking_received is not None:
    #         self.linking_received(linked_entities.values(), set())
    #
    #     entity_counter = 0
    #     for entity in missing_batch_entities:
    #         entity_counter += 1
    #         print(f'Linking single entity {entity_counter}/{len(missing_batch_entities)}')
    #         linked_entity, linking_info = self.entity_id(entity)
    #         if linked_entity is None:
    #             if not_found_entities is not None:
    #                 not_found_entities.add(entity)
    #             continue
    #
    #         batch_mapping[entity] = linked_entity
    #
    #     linked_entities.update(batch_mapping)
    # return batch_mapping






    # Frage an, persistiere
    # speichere

    #
    # pel = PersistentEntityLinker("test.csv")
    #
    # hallo = pel.entity_id("Hallo")
    #
    # pel.persist_entity(WikidataNamedEntity("Hallo", "", "Begrüßung"))
    #
    # hallo = pel.entity_id("Hallo")
    #
    # pel.persist_entity(WikidataNamedEntity("Hallo", "q123", "Begrüßung"))
    #
    # hallo = pel.entity_id("Hallo")

    # proxy = WikidataEntityLinkerProxy("test.csv")
    # entities = set()
    # not_found_entities = set()
    #
    # with open('/home/mapp/masterprojekt/embedding-evaluation/data/MEN_full.csv') as csvfile:
    #     reader = csv.reader(csvfile, delimiter=' ')
    #     next(reader)
    #     for row in reader:
    #         entities.add(WikidataEntityLinker.normalize(row[0]))
    #         entities.add(WikidataEntityLinker.normalize(row[1]))
    #
    # print(f'Linking {len(entities)} entities...')
    #
    # ret = proxy.entity_ids(list(entities), not_found_entities)
    # print("Found entities:")
    # for k, v in ret.items():
    #     print(k, v)
    #
    # print()
    # print("Not found entities:")
    # for item in not_found_entities:
    #     print(item)
    #
    # print(f"{len(ret)}/{len(entities)} of entities found.")
