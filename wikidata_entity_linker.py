import requests

from named_entity_linker import NamedEntityLinker, NamedEntity


class WikidataNamedEntity(NamedEntity):
    def __init__(self, entity, linked_entity, description):
        NamedEntity.__init__(self, entity, linked_entity)
        self.description = description

    def __str__(self):
        return '{}, {}, {}'.format(self.entity, self.linked_entity, self.description)


class WikidataEntityLinker(NamedEntityLinker):

    def link_entities(self, entities, not_found_entities):
        """
        Requests wikidata ids from wikidata using wikidata's API call to wbgetentities to each element in titles.
        :param entities: Collection of entities (strings). Each entity must contain at least one character.
            If entities contains more than one element, wikidata will not be able to normalize the entity.
            This may result in less entries found.
        :return: Returns Dictionary<entity, WikidataNamedEntity>. Entities, which could not be linked, will not be added
            to the dictionary.
        """
        linked_entities = dict()
        if not_found_entities is None:
            not_found_entities = set()
        entity_index = 0

        ENTITIES_PER_REQUEST = 500
        for i in range(0, len(entities), ENTITIES_PER_REQUEST):
            index_range = slice(i, min(len(entities), i + ENTITIES_PER_REQUEST))
            titles = "|".join(entities[index_range])
            normalize = True if len(entities) == 1 else False

            query_result = self._execute_query(titles, normalize)
            if query_result.status_code != 200:
                raise Exception("Failed to execute query")

            query_result_json = query_result.json()
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

                if description == 'Wikimedia disambiguation page':
                    not_found_entities.add(entities[entity_index])
                else:
                    linked_entities[entities[entity_index]] = WikidataNamedEntity(entities[entity_index], key,
                                                                                  description)
                entity_index += 1

        return linked_entities

    def _execute_query(self, titles, normalize):
        session = requests.Session()

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

    def entity_id(self, entity):

        if not entity:
            raise Exception("entity must not be None or empty.")

        return self.link_entities([entity], None)

    def entity_ids(self, entities, not_found_entities=None):
        missing_batch_entities = set()
        batch_mapping = self.link_entities(entities, missing_batch_entities)

        for entity in missing_batch_entities:
            single_mapping = self.entity_id(entity)
            if len(single_mapping) == 0 and not_found_entities is not None:
                not_found_entities.add(entity)
                continue

            for key, value in single_mapping.items():
                batch_mapping[entity] = value

        return batch_mapping


if __name__ == '__main__':
    # 1. Bilde eine
    nel = WikidataEntityLinker()
    # nel.entity_id("brmbrmi|Petal|Roof|Angela Merkel|Joschka Fischer|Door|roof|jdhfjk")

    # normalisiere Einträge vorab? z.B, Erster Buchstabe groß?

    not_found_entities = set()

    ret = nel.entity_ids(
        ["CAR", "Angela_Merkel", "Fridolin", "Joschka_Fischer", "Donald Trump", "Car", "Dandelion", "Mathias"],
        not_found_entities)
    print("Found entities:")
    for k, v in ret.items():
        print(k, v)

    print()
    print("Not found entities:")
    for item in not_found_entities:
        print(item)
