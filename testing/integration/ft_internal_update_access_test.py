import os
import tempfile
import valkey
from absl.testing import absltest
import utils


class FTInternalUpdateAccessTest(absltest.TestCase):
    """Test that FT.INTERNAL_UPDATE command is not accessible to customers."""

    def setUp(self):
        """Set up test environment with Valkey server."""
        # Get required paths from environment
        valkey_server_path = os.environ["VALKEY_SERVER_PATH"]
        valkey_search_path = os.environ["VALKEY_SEARCH_PATH"]
        
        # Create temporary directory for server
        self.temp_dir = tempfile.mkdtemp()
        self.stdout_file = open(os.path.join(self.temp_dir, "valkey.log"), "w")
        
        # Start server with search module
        self.server = utils.start_valkey_process(
            valkey_server_path=valkey_server_path,
            port=6379,
            directory=self.temp_dir,
            stdout_file=self.stdout_file,
            args={},
            modules={valkey_search_path: ""}
        )
        
        self.client = valkey.Valkey(host='localhost', port=6379, decode_responses=True)

    def tearDown(self):
        """Clean up test environment."""
        self.client.close()
        self.server.terminate()
        self.stdout_file.close()

    def test_ft_internal_update_blocked_for_customers(self):
        """Test that FT.INTERNAL_UPDATE command returns an error when called by customers."""
        # Try to call FT.INTERNAL_UPDATE directly
        with self.assertRaises(valkey.ResponseError) as context:
            self.client.execute_command("FT.INTERNAL_UPDATE", "CREATE", "test_data", "test_header")
        
        # Verify the error message indicates the command is not allowed
        error_message = str(context.exception).lower()
        self.assertIn("unknown", error_message, 
                     f"Expected 'unknown' in error message, got: {context.exception}")

    def test_ft_internal_update_variations_blocked(self):
        """Test various forms of FT.INTERNAL_UPDATE are blocked."""
        test_cases = [
            ["FT.INTERNAL_UPDATE"],
            ["FT.INTERNAL_UPDATE", "CREATE"],
            ["FT.INTERNAL_UPDATE", "CREATE", "data"],
            ["FT.INTERNAL_UPDATE", "CREATE", "data", "header"],
            ["ft.internal_update", "create", "data", "header"],  # lowercase
        ]
        
        for args in test_cases:
            with self.subTest(args=args):
                with self.assertRaises(valkey.ResponseError) as context:
                    self.client.execute_command(*args)
                
                error_message = str(context.exception).lower()
                self.assertIn("unknown", error_message,
                             f"Expected 'unknown' in error for args {args}, got: {context.exception}")

    def test_regular_ft_commands_still_work(self):
        """Test that regular FT commands still work normally."""
        # Create an index - this should work
        result = self.client.execute_command(
            "FT.CREATE", "testindex", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "embedding", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"
        )
        self.assertEqual(result, "OK")
        
        # List indexes - this should work
        result = self.client.execute_command("FT._LIST")
        self.assertIn("testindex", result)
        
        # Drop index - this should work
        result = self.client.execute_command("FT.DROPINDEX", "testindex")
        self.assertEqual(result, "OK")


if __name__ == '__main__':
    absltest.main()
