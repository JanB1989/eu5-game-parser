from eu5gameparser.clausewitz.parser import parse_text
from eu5gameparser.clausewitz.serializer import (
    normalized_document,
    normalized_text,
    render_document,
)


def test_render_document_round_trips_semantic_structure() -> None:
    text = """
REPLACE:cookery = {
\tis_foreign = no
\temployment_size = 3
\tunique_production_methods = {
\t\tpp_cookery = {
\t\t\twheat = 1.5
\t\t\tproduced = victuals
\t\t}
\t}
}
""".strip()

    rendered = render_document(parse_text(text))

    assert normalized_text(rendered) == normalized_text(text)
    assert "is_foreign = no" in rendered
    assert "wheat = 1.5" in rendered


def test_normalized_document_ignores_source_locations() -> None:
    left = parse_text("foo = { bar = yes }", path="left.txt")
    right = parse_text("foo = { bar = yes }", path="right.txt")

    assert normalized_document(left) == normalized_document(right)
