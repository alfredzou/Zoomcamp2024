
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data.columns = (data.columns.str.replace('ID$','_id', regex=True)
                                .str.lower())
    print(f"{data['lpep_pickup_date'].nunique()}")
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "vendor_id" in output.columns
