from custom_components.smart_villa import repairs


async def test_repairs_platform_imports_and_builds_flow():
    """HA 2026.9 logged: module 'homeassistant.helpers.issue_registry' has no attribute 'RepairsFlow'."""
    flow = await repairs.async_create_fix_flow(None, "connection", None)
    assert isinstance(flow, repairs.RepairsFlow)
