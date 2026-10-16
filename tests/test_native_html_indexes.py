"""Differential checks against the original full-node binding scans.

The reference overrides only candidate enumeration. Binding validation and
complete evidence outputs must agree, including rejected/ambiguous inputs.
"""
import copy
import json
import random
import unittest
from unittest.mock import patch

from or_pipeline import native_html as native


class FullScanDocument(native.Document):
    def _has_duplicate_keys(self, start, end):
        return any(n.duplicate_keys for n in self.nodes if start <= n.start < end)

    def _binding_owner_nodes(self, key):
        return self.nodes

    def _binding_query_nodes(self):
        return self.nodes

    def _binding_reference_nodes(self, query):
        return self.nodes


def raw(**updates):
    row = dict(model_permaslug='example/model', variant='free',
               date='2026-09-15T00:00:00Z', count=1,
               total_prompt_tokens=9007199254740995,
               total_completion_tokens=3,
               future={'nested': [None, {'value': 'kept'}]})
    row.update(updates)
    return row


def query(data, **updates):
    result = {'queryKey': ['model-page', 'appStats',
                          {'permaslug': 'example/model', 'variant': 'free'}],
              'state': {'data': data}, 'queryHash': 'unchanged'}
    result.update(updates)
    return result


def stats(**updates):
    result = {'analytics': [raw()], 'cachedAt': 1789516800000}
    result.update(updates)
    return result


def frames(*items):
    return ''.join(fid + ':' + json.dumps(value) + '\n' for fid, value in items)


def html(stream):
    return '<script>self.__next_f.push([1,' + json.dumps(stream) + '])</script>'


def outcome(call):
    try:
        return ('return', call())
    except Exception as exc:
        return ('raise', type(exc).__name__, str(exc))


def candidates(document):
    # Enumerate physical containers directly, so malformed/duplicate owners and
    # duplicate frames also exercise bind rather than only extraction guards.
    for array in document.nodes:
        if not isinstance(array.value, (list, dict)):
            continue
        for index, nodes in array.children.items():
            for node in nodes:
                if not isinstance(node.value, dict) or not any(k in node.value for k in native.CORE):
                    continue
                row = node.value
                date = row.get('date')
                for key in sorted(native.DAILY_KEYS):
                    yield {
                        'source_locator': f'rsc.offset[{array.start}].{key}[{json.dumps(index)}]',
                        'raw_fields': row, 'model_permaslug': row.get('model_permaslug'),
                        'variant_raw': row.get('variant'), 'date_raw': date,
                        'business_date': date[:10] if isinstance(date, str) else None,
                        **{k: row.get(k) for k in native.CORE}}


class NativeHtmlIndexTests(unittest.TestCase):
    def assert_equivalent(self, stream):
        body = html(stream)
        indexed_result = outcome(lambda: native.extract_model_daily(body))
        with patch.object(native, 'Document', FullScanDocument):
            scan_result = outcome(lambda: native.extract_model_daily(body))
        self.assertEqual(indexed_result, scan_result)
        try:
            indexed, scanned = native.Document(body), FullScanDocument(body)
        except (native.BindingError, ValueError, IndexError, RecursionError):
            return
        self.assertEqual(indexed.warnings, scanned.warnings)
        for row in candidates(indexed):
            self.assertEqual(outcome(lambda: indexed.bind(row)), outcome(lambda: scanned.bind(row)))
            for fid, nodes in indexed.frames.items():
                if any(node.start == int(native.LOC.fullmatch(row['source_locator'])[1]) for node in nodes):
                    refrow = dict(row, source_locator=row['source_locator'] + f'.rsc_ref[{fid}]')
                    self.assertEqual(outcome(lambda: indexed.bind(refrow)), outcome(lambda: scanned.bind(refrow)))
        for q in indexed.nodes:
            expected = [id(n) for n in indexed.nodes if q.start <= n.start < q.end
                        and isinstance(n.value, str) and native.REF.fullmatch(n.value)]
            self.assertEqual([id(n) for n in indexed._binding_reference_nodes(q)], expected)

    def test_nested_and_multiple_arrays_and_reference_chains(self):
        self.assert_equivalent(frames(('1', [query('$2'), {'unrelated': {'queryKey': ['other']}}]),
                                      ('2', stats(analytics='$3', model_chart=[raw()],
                                                  modelChart=[raw()], activityTimeSeries=[raw()])),
                                      ('3', '$4'), ('4', [raw()])))
        self.assert_equivalent(frames(('1', query('$2', queryKey='$5', state='$6')),
                                      ('2', stats(analytics='$3')), ('3', [raw()]),
                                      ('5', ['$7', '$8', '$9']), ('6', {'data': '$2'}),
                                      ('7', 'model-page'), ('8', 'appStats'),
                                      ('9', {'permaslug': 'example/model', 'variant': 'free'})))

    def test_duplicate_keys_at_every_binding_level(self):
        good = frames(('1', query(stats())))
        for old, new in [
            ('"count": 1', '"count": 2, "count": 1'),
            ('"value": "kept"', '"value": "first", "value": "kept"'),
            ('"cachedAt": 1789516800000', '"cachedAt": 1, "cachedAt": 1789516800000'),
            ('"variant": "free"', '"variant": "standard", "variant": "free"'),
            ('"queryHash": "unchanged"', '"queryHash": "first", "queryHash": "unchanged"'),
            ('"analytics": [', '"analytics": [], "analytics": ['),
            ('"state": {', '"state": {}, "state": {'),
            ('"queryKey": [', '"queryKey": [], "queryKey": [')]:
            with self.subTest(duplicate=old):
                self.assert_equivalent(good.replace(old, new))

    def test_duplicate_frames_ambiguous_owner_refs_and_error_order(self):
        fixtures = [
            frames(('1', query('$2')), ('2', stats(analytics='$3')), ('3', [raw()]), ('3', [raw()])),
            frames(('1', query('$2')), ('2', stats(analytics='$3')), ('3', [raw()]),
                   ('4', stats(analytics='$3'))),
            frames(('1', query('$2')), ('2', stats(analytics='$3')), ('3', [raw()]),
                   ('4', stats(analytics='$a')), ('a', '$3'), ('a', []),
                   ('5', stats(analytics='$b')), ('b', '$3'), ('b', [])),
            frames(('1', query('$2')), ('2', stats(analytics='$3')), ('3', [raw()]),
                   ('4', stats(analytics='$a')), ('a', '$b'), ('b', '$a')),
        ]
        for stream in fixtures:
            with self.subTest(stream=stream):
                self.assert_equivalent(stream)

    def test_ambiguous_query_refs_missing_state_and_nested_queries(self):
        for bad in [query('$2', state='$a'), query('$2', state={}),
                    {'queryKey': ['bad'], 'ref': '$2'},
                    query('$2', state={'data': '$a'}),
                    query({'nested': '$2'}), query('$2')]:
            with self.subTest(bad=bad):
                self.assert_equivalent(frames(('1', query('$2')), ('2', stats()),
                                              ('3', bad), ('a', {'data': '$2'}),
                                              ('a', '$2')))
        self.assert_equivalent(frames(('1', query(stats(extra=query('$2')))), ('2', stats())))

    def test_invalid_identity_midnight_date_and_core_values(self):
        changes = [{'variant': None}, {'variant': 'standard'}, {'model_permaslug': 'wrong'},
                   {'date': None}, {'date': 'broken'}, {'date': '2026-09-15T00:00:01Z'},
                   {'date': '2026-09-15T00:00:00+08:00'},
                   {'count': True}, {'count': -1}, {'count': 1.0}, {'count': None},
                   {'total_prompt_tokens': '9007199254740995'},
                   {'total_completion_tokens': -1}]
        for change in changes:
            with self.subTest(change=change):
                self.assert_equivalent(frames(('1', query(stats(analytics=[raw(**change)])))))

    def test_query_identity_scope_and_missing_owner_query(self):
        good = frames(('1', query(stats())))
        # First occurrence is query identity only; the native row stays valid.
        self.assert_equivalent(good.replace('"variant": "free"',
                                            '"variant": "standard", "variant": "free"', 1))
        for qk in [[], ['model-page', 'appStats'],
                   ['other-page', 'appStats', {'permaslug': 'example/model', 'variant': 'free'}],
                   ['model-page', 'appStats', {}],
                   ['model-page', 'appStats', {'permaslug': '', 'variant': 'free'}], '$a']:
            with self.subTest(query_key=qk):
                self.assert_equivalent(frames(('1', query(stats(), queryKey=qk))))
        self.assert_equivalent(frames(('1', stats())))
        self.assert_equivalent(frames(('1', [raw()])))

    def test_candidate_mismatches_preserve_validation_and_precedence(self):
        body = html(frames(('1', query(stats()))))
        indexed, scanned = native.Document(body), FullScanDocument(body)
        row = next(r for r in candidates(indexed) if '.analytics[' in r['source_locator'])
        self.assertEqual(indexed.bind(row), scanned.bind(row))
        changes = [{'source_locator': 'bad'}, {'source_locator': 'rsc.offset[0].analytics[0]'},
                   {'model_permaslug': 'wrong'}, {'variant_raw': None},
                   {'date_raw': 'wrong'}, {'business_date': '2020-01-01'},
                   {'count': 2}, {'total_prompt_tokens': 1}, {'total_completion_tokens': 1}]
        badraw = copy.deepcopy(row['raw_fields'])
        badraw['future']['nested'][1]['value'] = 'changed'
        changes += [{'raw_fields': badraw}, {'source_locator': row['source_locator'] + '.rsc_ref[f]'}]
        for change in changes:
            changed = dict(row, **change)
            with self.subTest(change=change):
                expected = outcome(lambda: scanned.bind(changed))
                self.assertEqual(expected[0], 'raise')
                self.assertEqual(outcome(lambda: indexed.bind(changed)), expected)

    def test_dictionary_daily_scope_and_nonrow_elements(self):
        self.assert_equivalent(frames(('1', query(stats(analytics={'named': raw()})))))
        self.assert_equivalent(frames(('1', query(stats(analytics=[None, '$2', raw(), {}, [raw()]]))),
                                      ('2', raw())))

    def test_malformed_streams_partial_nodes_and_text_frames(self):
        good = frames(('1', query(stats())))
        fake = '\n3:' + json.dumps([raw()]) + '\n'
        fixtures = [
            '0:{"partial":{"dup":1,"dup":2},"missing":}\n' + good,
            '0:{"partial":{"ref":"$1"},"missing":}\n' + good,
            '0:{"missing":\n' + good,
            '0:{"bad" 1}\n' + good,
            '0:{"bad": [1 2]}\n' + good,
            '0:{"bad":\n1:{"ref":"$1"}\n' + good,
            good + '2:T' + format(len(fake.encode()), 'x') + ',' + fake,
            good + '2:T100,short', good + '2:Tbad',
            '0:I["ignored resource"]\n' + good,
            '', 'no frames', '0:[',
        ]
        for stream in fixtures:
            with self.subTest(stream=stream):
                self.assert_equivalent(stream)

    def test_range_boundaries_and_original_order(self):
        document = native.Document(html(frames(('1', {'a': '$2', 'b': {'x': '$4'}}),
                                               ('2', {'x': 1, 'nested': {'a': '$1'}}))
                                        .replace('"x": "$4"', '"x": "$3", "x": "$4"')))
        self.assertTrue(document._duplicate_key_starts)
        for start in range(len(document.text) + 1):
            for end in (start, start + 1, len(document.text)):
                expected = any(n.duplicate_keys for n in document.nodes if start <= n.start < end)
                self.assertEqual(document._has_duplicate_keys(start, end), expected)
        # Also cover overlapping/repeated offsets from failed frame parsing.
        document.nodes.extend(reversed(document.nodes[:4]))
        document._build_binding_indexes()
        for node in document.nodes:
            expected = [n for n in document.nodes if node.start <= n.start < node.end
                        and isinstance(n.value, str) and native.REF.fullmatch(n.value)]
            self.assertEqual(list(document._binding_reference_nodes(node)), expected)

    def test_deterministic_generated_reference_graphs(self):
        rng = random.Random(20261016)
        for _ in range(30):
            entries = [('1', query('$2')), ('2', stats(analytics='$3')), ('3', [raw(), raw(count=2)])]
            for fid in ('4', '5', '6', '7', '8'):
                entries.append((fid, rng.choice([
                    {'ref': rng.choice(['$1', '$2', '$3', '$a'])},
                    query(rng.choice(['$2', '$a', {}])),
                    stats(analytics=rng.choice(['$3', '$a', []])),
                    ['$2', {'nested': '$3'}], {'other': [1, 2, 3]}])))
            if rng.choice([True, False]):
                entries.extend([('a', '$2'), ('a', {'data': '$2'})])
            rng.shuffle(entries)
            with self.subTest(entries=entries):
                self.assert_equivalent(frames(*entries))

    def test_binding_does_not_rescan_the_document(self):
        document = native.Document(html(frames(('1', query(stats())))))
        row = next(r for r in candidates(document) if '.analytics[' in r['source_locator'])
        expected = document.bind(row)

        class RejectIteration(list):
            def __iter__(self):
                raise AssertionError('bind rescanned all parsed nodes')

        document.nodes = RejectIteration(document.nodes)
        self.assertEqual(document.bind(row), expected)


if __name__ == '__main__':
    unittest.main()
