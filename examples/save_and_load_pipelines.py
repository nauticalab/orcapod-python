from orcapod import Pipeline, function_pod
from orcapod import sources, databases

database = databases.DeltaTableDatabase("./local_database")
source1 = sources.DictSource(
    [{"id": 0, "x": 5}, {"id": 1, "x": 10}, {"id": 2, "x": 15}],
    tag_columns=["id"],
    label="source1",
)
source1 = source1.cached(database)
source2 = sources.DictSource(
    [{"id": 0, "y": 3}, {"id": 2, "y": 6}, {"id": 4, "y": 9}],
    tag_columns=["id"],
    label="source2",
)
source2 = source2.cached(database)


pipeline = Pipeline("my_pipeline", database)


@function_pod("sum")
def take_sum(x: int, y: int) -> int:
    return x + y


with pipeline:
    result = take_sum.pod(source1.join(source2))

path = "./pipeline.json"
pipeline.save(path)


pipeline2 = Pipeline.load(path)
