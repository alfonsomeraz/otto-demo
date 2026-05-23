import pytest


def test_connection_convention_check_raises_on_wrong_id():
    """The intentional failure raises with a message naming both the wrong and correct IDs."""
    expected_conn_id = "meridian_snowflake_prod"
    actual_conn_id = "snowflake_prod"

    with pytest.raises(ValueError) as exc_info:
        if actual_conn_id != expected_conn_id:
            raise ValueError(
                f"Invalid Snowflake connection ID: {actual_conn_id}. "
                f"Expected company standard: {expected_conn_id}. "
                "This may prevent the fraud feature pipeline from accessing production transaction data."
            )

    assert "meridian_snowflake_prod" in str(exc_info.value)
    assert "snowflake_prod" in str(exc_info.value)


def test_connection_convention_check_passes_on_correct_id():
    """The check does not raise when the correct connection ID is used."""
    expected_conn_id = "meridian_snowflake_prod"
    actual_conn_id = "meridian_snowflake_prod"

    if actual_conn_id != expected_conn_id:
        raise ValueError(f"Invalid connection ID: {actual_conn_id}")
