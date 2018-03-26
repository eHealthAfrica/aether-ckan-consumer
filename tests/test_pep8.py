import unittest
import pycodestyle


class TestPEP8(unittest.TestCase):

    def test_conformance(self):
        ''' Test that we conform to PEP-8. '''

        style = pycodestyle.StyleGuide(paths=['consumer'])
        result = style.check_files()

        self.assertEqual(result.total_errors, 0,
                         'Found code style errors (and warnings).')
