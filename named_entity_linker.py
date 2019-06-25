from abc import ABC, abstractmethod
from enum import Enum, auto



class NamedEntity:
    """
    Simple pair consisting out of an entity (for example a word) and a linked_entity (for example a wikidata id).
    """
    def __init__(self, entity, linked_entity):
        self.entity = entity
        self.linked_entity = linked_entity

    def __str__(self):
        return '{}, {}'.format(self.entity, self.linked_entity)


class NamedEntityLinking(Enum):
    SUCCESS = auto(),
    NOT_FOUND = auto(),
    NO_LINKING_FOUND = auto()


class NamedEntityLinker(ABC):

    @abstractmethod
    def entity_id(self, entity):
        """

        :param entity:
        :return: A tuple<NamedEntity, NamedEntityLinking>. The first value contains the NamedEntity or None if entity
            could not be linked. The second value may give additional information on why the linking did not succeed.
        """
        pass

    @abstractmethod
    def entity_ids(self, entities, not_found_entities):
        pass
