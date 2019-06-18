from abc import ABC, abstractmethod

class NamedEntity:
    def __init__(self, entity, linked_entity):
        self.entity = entity
        self.linked_entity = linked_entity

    def __str__(self):
        return '{}, {}'.format(self.entity, self.linked_entity)

class NamedEntityLinker(ABC):

    @abstractmethod
    def entity_id(self, entity):
        pass
