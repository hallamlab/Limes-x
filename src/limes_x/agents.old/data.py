from typing import Callable, Any

from .generic import Outpost, Agent, type_name, Message, ACTION

# manages a local knowledgebase, can request information from primary data stores
class Clerk(Agent):
    def __init__(self, home_base: Outpost) -> None:
        clerks = home_base.GetAgent(type_name(Clerk))
        assert len(clerks) == 0, f"there is already a clerk at this location"
        super().__init__(home_base)

        def _make_filter(attributes: set, properties: dict):
            def _data_stores():
                def _matches(d: Datastore):
                    return all(a in d.attributes for a in attributes) and all(properties[pk] == d.properties.get(pk) for pk in properties)
                stores = self._home_base.GetAgent(type_name(Datastore))
                stores: list[Agent] = [a for a in stores if isinstance(a, Datastore) and _matches(a)]
                return stores
            return _data_stores
        self.RegisterData = self.CreateOnDelegateCallback(
            _make_filter({"writeable"}, {}), "RegisterData", "no datastores",
        )
        self.Query = self.CreateOnDelegateCallback(
            _make_filter(set(), {}), "Query", "no datastores",
        )

class Datastore(Agent):
    def __init__(
            self,
            onInsert: Callable,
            onQuery: Callable,
            description: tuple[set, dict],
            home_base: Outpost
        ) -> None:
        super().__init__(home_base)
        self.attributes, self.properties = description

        self.RegisterData = onInsert
        self.Query = onQuery

