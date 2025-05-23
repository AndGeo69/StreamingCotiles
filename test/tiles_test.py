import unittest
from alg.TILES import TILES
from alg.eTILES import eTILES
import shutil
import glob
import os


class TilesTestCase(unittest.TestCase):

    def test_eTILES(self):
        base = os.path.dirname(os.path.abspath(__file__))
        directory = "eres"

        # try:
        # Ensure the directory is created
        os.makedirs(directory, exist_ok=True)  # `exist_ok=True` prevents the FileExistsError

        et = eTILES(filename="%s/mydata/NetworkSegmentETiles30.tsv" % base, obs=1, path=directory)
        et.execute()

        count = 0
        for _ in glob.glob(f"{directory}/graph*"):
            count += 1
        self.assertEqual(count, 683)

        # finally:
        #     # Cleanup: Remove the directory if it exists
        #     if os.path.exists(directory):
        #         shutil.rmtree(directory)

    def test_TILES(self):
        base = os.path.dirname(os.path.abspath(__file__))
        os.makedirs("res")
        et = TILES(filename="%s/sample_net_tiles.tsv" % base, obs=1, path="res")
        et.execute()

        count = 0
        for _ in glob.glob("res/graph*"):
            count += 1
        self.assertEqual(count, 6)

        shutil.rmtree("res")

        os.makedirs("res2")
        et = t.TILES(filename="%s/sample_net_tiles.tsv" % base, obs=1, path="res2", ttl=1)
        et.execute()

        count = 0
        for _ in glob.glob("res2/graph*"):
            count += 1
        self.assertEqual(count, 6)

        shutil.rmtree("res2")

        os.makedirs("res3")
        et = t.TILES(filename="%s/sample_net_tiles.tsv" % base, obs=1, path="res3", ttl=2)
        et.execute()

        count = 0
        for _ in glob.glob("res3/graph*"):
            count += 1
        self.assertEqual(count, 6)

        shutil.rmtree("res3")

    def test_TILES_rem(self):
        base = os.path.dirname(os.path.abspath(__file__))
        os.makedirs("res4")
        et = t.TILES(filename="%s/gen_simple.tsv" % base, obs=30, path="res4", ttl=90)
        et.execute()

        count = 0
        for _ in glob.glob("res4/graph*"):
            count += 1

        self.assertEqual(count, 34)
        shutil.rmtree("res4")


if __name__ == '__main__':
    unittest.main()
