import unittest

from datastation.dataverse.metrics_collect import extract_size_str


class TestMetricsCollect(unittest.TestCase):

    def test_extract_storage_size(self):
        normal_msg = "Total size of the files stored in this dataverse: 43,638,426,561 bytes"
        self.assertEqual(extract_size_str(normal_msg), "43638426561")

        allowed_variation_msg = "Some text that is ignored ... dataverse: 12.345 bytes"
        self.assertEqual(extract_size_str(allowed_variation_msg), "12345")

        with self.assertRaises(AttributeError):
            extract_size_str("")
        with self.assertRaises(AttributeError):
            extract_size_str("12345")
        with self.assertRaises(AttributeError):
            extract_size_str("dataverse: 12345")
        with self.assertRaises(AttributeError):
            extract_size_str("12345 bytes")


if __name__ == '__main__':
    unittest.main()
