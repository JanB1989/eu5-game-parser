from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from eu5gameparser.domain.advancements import AdvancementData, load_advancement_data
from eu5gameparser.domain.buildings import BuildingData, load_building_data
from eu5gameparser.domain.goods import GoodsData, build_goods_summary, load_goods_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH


@dataclass(frozen=True)
class Eu5Data:
    advancements: pl.DataFrame
    buildings: pl.DataFrame
    goods: pl.DataFrame
    goods_summary: pl.DataFrame
    production_methods: pl.DataFrame
    goods_flow_nodes: pl.DataFrame
    goods_flow_edges: pl.DataFrame
    advancement_data: AdvancementData
    building_data: BuildingData
    goods_data: GoodsData
    warnings: list[str] = field(default_factory=list)


def load_eu5_data(
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> Eu5Data:
    advancement_data = load_advancement_data(profile=profile, load_order_path=load_order_path)
    goods_data = load_goods_data(profile=profile, load_order_path=load_order_path)
    building_data = load_building_data(
        profile=profile, load_order_path=load_order_path, goods_data=goods_data
    )
    return Eu5Data(
        advancements=advancement_data.advancements,
        buildings=building_data.buildings,
        goods=goods_data.goods,
        goods_summary=build_goods_summary(goods_data.goods, building_data.production_methods),
        production_methods=building_data.production_methods,
        goods_flow_nodes=building_data.goods_flow_nodes,
        goods_flow_edges=building_data.goods_flow_edges,
        advancement_data=advancement_data,
        building_data=building_data,
        goods_data=goods_data,
        warnings=[*advancement_data.warnings, *building_data.warnings, *goods_data.warnings],
    )
