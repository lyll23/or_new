import json
import unittest

from or_pipeline.native_html import extract_model_daily


def raw(variant="free"):
    return {"model_permaslug": "example/model-v1", "variant": variant,
            "date": "2026-09-15T00:00:00Z", "count": 0,
            "total_prompt_tokens": 9007199254740995,
            "total_completion_tokens": 3, "future_field": {"kept": True}}


def query(stats, variant="free"):
    return {"queryKey": ["model-page", "appStats", {
        "permaslug": "example/model-v1", "variant": variant}],
        "state": {"data": stats}}


def html(frames):
    return '<script>self.__next_f.push([1,' + json.dumps(frames) + '])</script>'


class NativeHtmlTests(unittest.TestCase):
    def test_exact_owner_and_raw_precision(self):
        row = raw()
        result = extract_model_daily(html('1:' + json.dumps(query({
            "model_chart": [row], "cachedAt": 1789516800000})) + '\n'))
        self.assertEqual(result['rows'][0]['raw'], row)
        self.assertEqual(result['rows'][0]['evidence']['query_identity_resolved']['variant'], 'free')
        self.assertEqual(result['issues'], [])

    def test_adjacent_cache_does_not_supply_owner_cache(self):
        result = extract_model_daily(html('1:' + json.dumps([
            query({"model_chart": [raw()]}),
            query({"cachedAt": 1789516800000, "other": []})]) + '\n'))
        self.assertEqual(result['rows'], [])
        self.assertTrue(result['issues'])

    def test_missing_or_conflicting_variant_not_defaulted(self):
        for replacement in (None, 'standard'):
            row = raw()
            if replacement is None:
                row.pop('variant')
            else:
                row['variant'] = replacement
            result = extract_model_daily(html('1:' + json.dumps(query({
                "model_chart": [row], "cachedAt": 1789516800000})) + '\n'))
            self.assertEqual(result['rows'], [])

    def test_missing_count_not_zero(self):
        row = raw()
        row.pop('count')
        result = extract_model_daily(html('1:' + json.dumps(query({
            "model_chart": [row], "cachedAt": 1789516800000})) + '\n'))
        self.assertEqual(result['rows'], [])

    def test_flight_reference(self):
        frames = '1:' + json.dumps(query('$2')) + '\n2:' + json.dumps({
            "model_chart": '$3', "cachedAt": 1789516800000}) + '\n3:' + json.dumps([raw()]) + '\n'
        result = extract_model_daily(frames, 'text/x-component')
        self.assertEqual(len(result['rows']), 1)
        self.assertTrue(result['rows'][0]['evidence']['reference_evidence'])

    def test_text_frame_fake_reference_cannot_be_resolved(self):
        fake = '\n3:' + json.dumps([raw()]) + '\n'
        frames = '1:' + json.dumps(query({"model_chart": '$3', "cachedAt": 1789516800000})) + '\n'
        frames += '2:T' + format(len(fake.encode('utf-8')), 'x') + ',' + fake
        result = extract_model_daily(frames, 'text/x-component')
        self.assertEqual(result['rows'], [])

    def test_duplicate_frame_and_duplicate_key_rejected(self):
        good = '1:' + json.dumps(query({"model_chart": [raw()], "cachedAt": 1789516800000})) + '\n'
        self.assertEqual(extract_model_daily(html(good + good))['rows'], [])
        duplicated_key = good.replace('"cachedAt": 1789516800000', '"cachedAt": 1, "cachedAt": 1789516800000')
        self.assertEqual(extract_model_daily(html(duplicated_key))['rows'], [])

    def test_duplicate_row_or_nested_field_is_never_silently_last_wins(self):
        good = '1:' + json.dumps(query({"model_chart": [raw()], "cachedAt": 1789516800000})) + '\n'
        for broken in (good.replace('"count": 0', '"count": 999, "count": 0'),
                       good.replace('"kept": true', '"kept": false, "kept": true')):
            result = extract_model_daily(html(broken))
            self.assertEqual(result['rows'], [])
            self.assertTrue(any('duplicate_native_row_json_key' in x for x in result['issues']))


if __name__ == '__main__':
    unittest.main()
