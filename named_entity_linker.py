from abc import ABC, abstractmethod

class NamedEntity:
    """
    Simple pair consisting out of an entity (for example a word) and a linked_entity (for example a wikidata id).
    """
    def __init__(self, entity, linked_entity):
        self.entity = entity
        self.linked_entity = linked_entity

    def __str__(self):
        return '{}, {}'.format(self.entity, self.linked_entity)

class NamedEntityLinker(ABC):

    @abstractmethod
    def entity_id(self, entity):
        pass

    @abstractmethod
    def entity_ids(self, entities, not_found_entities):
        pass
