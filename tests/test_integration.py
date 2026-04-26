import os

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data


@pytest.mark.integration
def test_parse_real_install_when_enabled() -> None:
    if os.environ.get("EU5_RUN_INTEGRATION") != "1":
        pytest.skip("Set EU5_RUN_INTEGRATION=1 to parse the local EU5 install.")

    config = ParserConfig.from_env()
    if not config.game_root.exists():
        pytest.skip(f"EU5 game root not found: {config.game_root}")

    data = load_building_data(config)

    assert data.categories.height > 0
    assert data.buildings.height > 0
    assert data.production_methods.height > 0
    assert data.goods_flow_nodes.height > 0
    assert data.goods_flow_edges.height > 0
