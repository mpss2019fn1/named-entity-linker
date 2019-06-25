import requests
import csv
import math
import os

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

    def _link_entities(self, entities, not_found_entities, session):
        """
        Requests wikidata ids to every entity in entities.
        The linking is performed using wikidata's API call to 'wbgetentities'.
        Entities which can not be linked exactly (link to a disambiguation page) will be ignored and added to
        not_found_entities.

        :param entities: Collection of entities (strings). Each entity must contain at least one character.
            WikidataEntityLinker will try to link several entities at once (in blocks of max. 50 entities per query).
            When fetching several ids at once, normalization (for example converting a word to ist base form) is not
            possible. This may result in less entries being found.
        :param not_found_entities: Expects a list wich will be used to store entities which could not be linked.
        :return: Returns Dictionary<entity, WikidataNamedEntity>. Entities, which could not be linked, will not be added
            to the dictionary.
        """
        linked_entities = dict()
        if not_found_entities is None:
            not_found_entities = set()

        entity_index = 0
        entities_per_request = 50
        batch_count = math.ceil(len(entities) / entities_per_request)
        for i in range(0, len(entities), entities_per_request):
            # split entities in batches of max entities_per_request per request.
            index_range = slice(i, min(len(entities), i + entities_per_request))
            titles = "|".join(entities[index_range])
            normalize = True if len(entities) == 1 else False

            print(f"Processing batch {i // entities_per_request + 1}/{batch_count}")
            query_result = self._execute_query(titles, normalize, session)
            if query_result.status_code != 200:
                raise Exception("http request to fetch wikidata ids to entity failed")

            # parse results
            query_result_json = query_result.json()

            if 'entities' not in query_result_json:
                raise Exception("http request failed, Key 'entities' not found in result")

            for key, value in query_result_json['entities'].items():
                description = None
                if key[0] != 'Q':
                    not_found_entities.add(value['title'])
                    continue
                try:
                    description = value['descriptions']['en']['value']
                except KeyError:
                    pass

                while entities[entity_index] in not_found_entities:
                    entity_index += 1

                if description is not None and 'disambiguation page' in description:
                    not_found_entities.add(entities[entity_index])
                else:
                    linked_entities[entities[entity_index]] = WikidataNamedEntity(entities[entity_index], key,
                                                                                  description)
                entity_index += 1

        return linked_entities

    def _execute_query(self, titles, normalize, session=requests.Session()):
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

        return session.get(url=wikidata_api_url, params=params)

    def entity_id(self, entity, session=requests.Session()):
        """
        Tries to link a single entity
        :param session:
        :param entity:
        :return: Dictionary<entity, WikidataNamedEntity>; Dictionary is empty if entity could not be linked.
        """
        if not entity:
            raise Exception("entity must not be None or empty.")

        result = self._link_entities([entity], not_found_entities=None, session=session)
        assert len(result) < 2, "An entity should only be linked to max one id."

        if len(result) == 0:
            return None, NamedEntityLinking.NOT_FOUND

        return list(result.values())[0], NamedEntityLinking.SUCCESS

    def entity_ids(self, entities, not_found_entities=None, session=requests.Session()):
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
        missing_batch_entities = set()
        batch_mapping = self._link_entities(entities, missing_batch_entities, session)

        entity_counter = 0
        for entity in missing_batch_entities:
            entity_counter += 1
            print(f'Linking single entity {entity_counter}/{len(missing_batch_entities)}')
            linked_entity, linking_info = self.entity_id(entity, session)
            if linked_entity is None:
                if not_found_entities is not None:
                    not_found_entities.add(entity)
                continue

            batch_mapping[entity] = linked_entity

        return batch_mapping

    @staticmethod
    def normalize(entity):
        return entity.capitalize()


class WikidataEntityLinkerProxy(NamedEntityLinker):
    def __init__(self, filename):
        """

        :param filename: Path to persistent storage
        """
        self._wikidata_entity_linker = WikidataEntityLinker()
        self._persistent_entity_linker = PersistentEntityLinker(filename)
        self._session = requests.Session()

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
                                                                  not_matched_entities_using_wikidata, self._session)

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


if __name__ == '__main__':
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

    proxy = WikidataEntityLinkerProxy("test.csv")
    entities = set()
    not_found_entities = set()

    with open('/home/mapp/masterprojekt/embedding-evaluation/data/MEN_full.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ')
        next(reader)
        for row in reader:
            entities.add(WikidataEntityLinker.normalize(row[0]))
            entities.add(WikidataEntityLinker.normalize(row[1]))

    print(f'Linking {len(entities)} entities...')

    ret = proxy.entity_ids(list(entities), not_found_entities)
    print("Found entities:")
    for k, v in ret.items():
        print(k, v)

    print()
    print("Not found entities:")
    for item in not_found_entities:
        print(item)

    print(f"{len(ret)}/{len(entities)} of entities found.")

    # with open('/home/mapp/masterprojekt/embedding-evaluation/data/MEN_full.csv') as csvfile:
    #     reader = csv.reader(csvfile, delimiter=' ')
    #     next(reader)
    #     for row in reader:
    #         entities.add(WikidataEntityLinker.normalize(row[0]))
    #         entities.add(WikidataEntityLinker.normalize(row[1]))
    #
    # print(f'Linking {len(entities)} entities...')
    # nel = WikidataEntityLinker()
    # not_found_entities = set()
    #
    # ret = nel.entity_ids(list(entities), not_found_entities)
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
