import unittest
import kv


class TestKV(unittest.TestCase):
    def setUp(self):
        self.database = kv.KVStorage(":memory:", "test")

    def test_put(self):
        self.database.put("test", "test value")
        self.assertEqual("test value", self.database.get("test"))

    def test_delete(self):
        self.database.put("test", "test value")
        self.database.delete("test")
        self.assertIsNone(self.database.get("test"))

    def test_contains(self):
        self.database.put("test", "test value")
        self.assertTrue(self.database.contains("test"))
        self.assertFalse(self.database.contains("t e s t"))

    def test_compress(self):
        self.database = kv.KVStorage(":memory:", "test", compress_value=True)
        self.database.put("test", "value" * 100)
        self.assertEqual("value" * 100, self.database.get("test"))

    def test_overwrite(self):
        self.database.put("test", "value 1")
        self.assertEqual(self.database.get("test"), "value 1")
        self.database.put("test", "value 2")
        self.assertEqual(self.database.get("test"), "value 2")

    def test_type_mismatch(self):
        with self.assertRaises(TypeError):
            self.database.put("test", 1)
