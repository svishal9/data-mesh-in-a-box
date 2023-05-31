import unittest
from ...src.hello_world import print_hello


class MyTestCase(unittest.TestCase):
    def test_hello_world(self):
        res = print_hello()
        self.assertEqual(res, "Hello World")


if __name__ == '__main__':
    unittest.main()
