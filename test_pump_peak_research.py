import unittest
from pump_peak_research import HOUR_MS, extract_events, report


class PumpPeakResearchTests(unittest.TestCase):
    def test_one_peak_one_reversal(self):
        rows=[]
        for i in range(90):
            price=100.0
            if i==51:
                price=112.0
            if i>=52:
                price=105.0
            rows.append((i*HOUR_MS,price,price,price,price))
        events=extract_events(rows)
        reversed_events=[x for x in events if x["reversal"]]
        self.assertTrue(reversed_events)
        self.assertAlmostEqual(reversed_events[0]["rise48"],12.0)
        self.assertGreaterEqual(reversed_events[0]["drop6h"],4.0)
    def test_flat_series_has_no_events(self):
        rows=[(i*HOUR_MS,100,100,100,100) for i in range(100)]
        self.assertEqual(extract_events(rows),[])
    def test_missing_hour_skipped(self):
        rows=[(i*HOUR_MS,100,100,100,100) for i in range(90) if i!=47]
        self.assertEqual(extract_events(rows),[])
        self.assertEqual(report({"TEST":rows})["reversed"],0)

if __name__=="__main__":
    unittest.main()
