from valkey_search_test_case import ValkeySearchTestCaseBase
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker


class TestFTInternalUpdateAccess(ValkeySearchTestCaseBase):

    """
    Test various forms of FT.INTERNAL_UPDATE are blocked.
    Command should only be accessed with admin permissions
    """
    def test_ft_internal_update_variations_blocked(self):

        # Remove @admin permissions to simulate customer access
        self.client.execute_command("ACL", "SETUSER", "default", "-@admin")
        
        test_cases = [
            ["FT.INTERNAL_UPDATE"],
            ["FT.INTERNAL_UPDATE", "CREATE"],
            ["FT.INTERNAL_UPDATE", "CREATE", "data"],
            ["FT.INTERNAL_UPDATE", "CREATE", "data", "header"],
            ["ft.internal_update", "create", "data", "header"],  # lowercase
        ]
        
        for args in test_cases:
            with pytest.raises(valkey.ResponseError) as exc_info:
                self.client.execute_command(*args)
            
            error_message = str(exc_info.value).lower()
            assert "no permissions" in error_message, f"Expected 'no permissions' in error for args {args}, got: {exc_info.value}"
