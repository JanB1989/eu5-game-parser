from eu5gameparser.clausewitz.parser import parse_text
from eu5gameparser.clausewitz.syntax import CList


def test_parser_handles_comments_nested_lists_repeated_keys_and_comparisons() -> None:
    document = parse_text(
        """
        # comment
        building = {
            name = "Stone Mason"
            repeated = one
            repeated = two
            active = yes
            amount = -1.5
            list = { alpha beta }
            trigger = {
                num_roads > 0
            }
        }
        """
    )

    building = document.entries[0].value
    assert isinstance(building, CList)
    assert building.first("name") == "Stone Mason"
    assert building.values("repeated") == ["one", "two"]
    assert building.first("active") is True
    assert building.first("amount") == -1.5

    list_value = building.first("list")
    assert isinstance(list_value, CList)
    assert list_value.items == ["alpha", "beta"]

    trigger = building.first("trigger")
    assert isinstance(trigger, CList)
    assert trigger.entries[0].key == "num_roads"
    assert trigger.entries[0].op == ">"
    assert trigger.entries[0].value == 0
