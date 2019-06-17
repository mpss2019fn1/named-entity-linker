from abc import ABC, abstractmethod


class NamedEntityLinker(ABC):

    @abstractmethod
    def entity_id(self, entity):
        pass
