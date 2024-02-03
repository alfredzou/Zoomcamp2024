from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    print(f"zero passenger trips: {df[df['passenger_count']<=0].shape[0]}")
    print(f"zero distance trips: {df[df['trip_distance']<=0].shape[0]}")
    print(f"zero passenger and distance trips: {df[((df['passenger_count']<=0) & (df['trip_distance']<=0))].shape[0]}")
    print(f"zero passenger or distance trips: {df[((df['passenger_count']<=0) | (df['trip_distance']<=0))].shape[0]}")
    return df[(df['passenger_count']>0) & (df['trip_distance']>0)]


@test
def test_no_empty_trips(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output[output['passenger_count']<=0].shape[0]==0

@test
def test_no_distance_trips(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output[output['trip_distance']<=0].shape[0]==0

