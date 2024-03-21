import time
from datetime import datetime

from datastation.common.batch_processing import BatchProcessor, get_pids


class TestBatchProcessor:

    def test_process_pids(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        pids = ["A", "B", "C"]
        callback = lambda pid: print(pid)
        batch_processor.process_pids(pids, callback)
        captured = capsys.readouterr()
        assert captured.out == "A\nB\nC\n"
        assert len(caplog.records) == 7
        assert (caplog.records[0].message == 'Start batch processing on 3 entries')
        assert (caplog.records[1].message == 'Processing 1 of 3 entries: A')
        assert (caplog.records[2].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[3].message == 'Processing 2 of 3 entries: B')
        assert (caplog.records[4].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[5].message == 'Processing 3 of 3 entries: C')
        assert (caplog.records[6].message == 'Batch processing ended: 3 entries processed')

    def test_process_dicts_with_pids(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        objects = [{"PID": "a", "param": "value1"},
                   {"PID": "b", "param": "value2"},
                   {"PID": "a", "param": "value3"}]
        batch_processor.process_pids(objects, lambda obj: print(obj))
        captured = capsys.readouterr()
        assert captured.out == ("{'PID': 'a', 'param': 'value1'}\n"
                                "{'PID': 'b', 'param': 'value2'}\n"
                                "{'PID': 'a', 'param': 'value3'}\n")
        assert len(caplog.records) == 7
        assert (caplog.records[0].message == 'Start batch processing on 3 entries')
        assert (caplog.records[1].message == 'Processing 1 of 3 entries: a')
        assert (caplog.records[2].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[3].message == 'Processing 2 of 3 entries: b')
        assert (caplog.records[4].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[5].message == 'Processing 3 of 3 entries: a')
        assert (caplog.records[6].message == 'Batch processing ended: 3 entries processed')

    def test_empty_stream(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        objects = map(lambda rec: rec[''], [])  # lazy empty iterator
        batch_processor.process_pids(objects, lambda obj: print(obj))
        assert capsys.readouterr().out == ""
        assert caplog.text == (
            'INFO     root:batch_processing.py:62 Start batch processing on unknown number of entries\n'
            'INFO     root:batch_processing.py:91 Batch processing ended: 0 entries processed\n')
        assert len(caplog.records) == 2
        assert (caplog.records[0].message == 'Start batch processing on unknown number of entries')
        assert (caplog.records[1].message == 'Batch processing ended: 0 entries processed')

    def test_single_item(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        batch_processor.process_entries(['a'], lambda obj: print(obj))
        assert capsys.readouterr().out == "a\n"
        assert caplog.text == (
            'INFO     root:batch_processing.py:60 Start batch processing on 1 entries\n'
            'INFO     root:batch_processing.py:91 Batch processing ended: 1 entries processed\n')
        assert len(caplog.records) == 2
        assert (caplog.records[0].message == 'Start batch processing on 1 entries')
        assert (caplog.records[1].message == 'Batch processing ended: 1 entries processed')

    def test_no_entries(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        batch_processor.process_pids(None, lambda obj: print(obj))
        assert capsys.readouterr().out == ""
        assert caplog.text == 'INFO     root:batch_processing.py:56 Nothing to process\n'
        assert len(caplog.records) == 1
        assert (caplog.records[0].message == 'Nothing to process')

    def test_empty_list(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        batch_processor.process_pids([], lambda obj: print(obj))
        assert capsys.readouterr().out == ""
        assert caplog.text == ('INFO     root:batch_processing.py:60 Start batch processing on 0 entries\n'
                               'INFO     root:batch_processing.py:91 Batch processing ended: 0 entries '
                               'processed\n')
        assert len(caplog.records) == 2
        assert (caplog.records[0].message == 'Start batch processing on 0 entries')
        assert (caplog.records[1].message == 'Batch processing ended: 0 entries processed')

    def test_process_objects_without_pids(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor()
        objects = [{"no-pid": "1", "param": "value1"},
                   {"no-pid": "2", "param": "value2"},
                   {"no-pid": "1", "param": "value3"}]
        batch_processor.process_pids(objects, lambda obj: print(obj['param']))
        captured = capsys.readouterr()
        assert captured.out == "value1\nvalue2\nvalue3\n"
        assert len(caplog.records) == 7
        assert (caplog.records[0].message == 'Start batch processing on 3 entries')
        assert (caplog.records[1].message == 'Processing 1 of 3 entries')
        assert (caplog.records[2].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[3].message == 'Processing 2 of 3 entries')
        assert (caplog.records[4].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[5].message == 'Processing 3 of 3 entries')
        assert (caplog.records[6].message == 'Batch processing ended: 3 entries processed')

    def test_process_pids_with_wait_on_iterator(self, capsys, caplog):
        caplog.set_level('DEBUG')
        batch_processor = BatchProcessor(wait=0.1)

        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        pids = map(as_is, ["a", "b", "c"])
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called alternated with callback
        assert captured.out == "lazy-a\na\nlazy-b\nb\nlazy-c\nc\n"
        assert (end_time - start_time).total_seconds() >= 0.5
        assert len(caplog.records) == 7
        assert (caplog.records[0].message == 'Start batch processing on unknown number of entries')
        assert (caplog.records[1].message == 'Processing entry number 1: a')
        assert (caplog.records[2].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[3].message == 'Processing entry number 2: b')
        assert (caplog.records[4].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[5].message == 'Processing entry number 3: c')
        assert (caplog.records[6].message == 'Batch processing ended: 3 entries processed')

    def test_process_pids_with_wait_on_list(self, capsys):
        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        batch_processor = BatchProcessor(wait=0.1)
        pids = [as_is(rec) for rec in ["1", "2", "3"]]
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called repeatedly before callback is called repeatedly
        # this illustrates the "don't" of search_api.search.return
        assert captured.out == "lazy-1\nlazy-2\nlazy-3\n1\n2\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.2

    def test_process_exception(self, caplog):
        caplog.set_level('DEBUG')

        def raise_second(rec):
            if rec == "b":
                raise Exception("b is not allowed")

        batch_processor = BatchProcessor(wait=0.1)
        pids = ["a", "b", "c"]
        callback = lambda pid: raise_second(pid)
        batch_processor.process_pids(pids, callback)
        assert len(caplog.records) == 7
        assert (caplog.records[0].message == 'Start batch processing on 3 entries')
        assert (caplog.records[1].message == 'Processing 1 of 3 entries: a')
        assert (caplog.records[2].message == 'Waiting 0.1 seconds before processing next entry')
        assert (caplog.records[3].message == 'Processing 2 of 3 entries: b')
        assert (caplog.records[4].message == 'Exception occurred on entry nr 2')
        assert (caplog.records[5].message == 'Stop processing because of an exception: b is not allowed')

        # actually the 2nd entry is not processed
        assert (caplog.records[6].message == 'Batch processing ended: 2 entries processed')

    def test_continue_after_exception(self, caplog):
        caplog.set_level('DEBUG')

        def raise_second(rec):
            if rec == "b":
                raise Exception("b is not allowed")

        batch_processor = BatchProcessor(wait=0.1, fail_on_first_error=False)
        pids = ["a", "b", "c"]
        callback = lambda pid: raise_second(pid)
        batch_processor.process_pids(pids, callback)
        assert len(caplog.records) == 9
        assert (caplog.records[5].message == 'fail_on_first_error is False, continuing...')
        assert (caplog.records[6].message == 'Waiting 0.1 seconds before processing next entry')
        # see other tests for other lines

    def test_get_single_pid(self):
        pids = get_pids('doi:10.5072/DAR/ATALUT')
        assert pids == ['doi:10.5072/DAR/ATALUT']

    def test_get_pids_from_file(self, tmp_path):
        pids_file = tmp_path / "pids.txt"
        with open(pids_file, 'w') as f:
            f.write('doi:10.5072/DAR/ATALUT\ndoi:10.17026/dans-xfg-s8q3\n')
            f.close()
        pids = get_pids(pids_file)
        assert pids == ['doi:10.5072/DAR/ATALUT', 'doi:10.17026/dans-xfg-s8q3']

    def test_get_pids_from_empty_file(self, tmp_path):
        pids_file = tmp_path / "empty.txt"
        open(pids_file, 'w').close()
        assert get_pids(pids_file) == []

    def test_no_pids_or_file(self):
        assert get_pids(None) == []
