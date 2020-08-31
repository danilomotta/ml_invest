from kedro.pipeline import Pipeline, node

# from .nodes import data_extracted
from .nodes_ibov import get_ibov_urls, get_ibov_data, get_timeline, agg_ibov_csv

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=get_timeline,
                inputs=["params:ibov_from_year"],
                outputs="timeline",
                name="get_timeline"
            ),
            node(
                func=get_ibov_urls,
                inputs=["timeline", "params:ibov_link"],
                outputs="ibov_urls",
                name="get_ibov_urls"
            ),
            node(
                func=get_ibov_data,
                inputs=["ibov_urls", "last_updated",  "params:ibov_link"],
                outputs="ibov_csv",
                name="get_ibov_data",
                confirms="ibov_urls"
            ),
            node(
                func=agg_ibov_csv,
                inputs=["ibov_csv", "last_updated"],
                outputs=["ibov_dataset", "updated"],
                name="agg_ibov_hist",
            )
        ]
    )