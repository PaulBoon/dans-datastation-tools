import os
import time
from datetime import datetime

from datastation.common.batch_processing import BatchProcessor, get_pids


class TestBatchProcessor:

    def test_process_pids(self, capsys):
        batch_processor = BatchProcessor()
        pids = ["1", "2", "3"]
        callback = lambda pid: print(pid)
        batch_processor.process_pids(pids, callback)
        captured = capsys.readouterr()
        assert captured.out == "1\n2\n3\n"

    def test_process_objects_with_pids(self, capsys):
        batch_processor = BatchProcessor()
        objects = [{"PID": "1", "param": "value1"},
                   {"PID": "2", "param": "value2"},
                   {"PID": "1", "param": "value3"}]
        batch_processor.process_pids(objects, lambda obj: print(obj))
        captured = capsys.readouterr()
        assert captured.out == ("{'PID': '1', 'param': 'value1'}\n"
                                "{'PID': '2', 'param': 'value2'}\n"
                                "{'PID': '1', 'param': 'value3'}\n")

    def test_process_objects_without_pids(self, capsys):
        batch_processor = BatchProcessor()
        objects = [{"no-pid": "1", "param": "value1"},
                   {"no-pid": "2", "param": "value2"},
                   {"no-pid": "1", "param": "value3"}]
        batch_processor.process_pids(objects, lambda obj: print(obj['param']))
        captured = capsys.readouterr()
        assert captured.out == ("value1\nvalue2\nvalue3\n")

    def test_process_pids_with_wait_on_iterator(self, capsys):
        batch_processor = BatchProcessor(wait=0.1)

        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        pids = map(as_is, ["1", "2", "3"])
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called alternated with callback
        assert captured.out == "lazy-1\n1\nlazy-2\n2\nlazy-3\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.5

    def test_process_pids_with_wait_on_list(self, capsys):
        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        batch_processor = BatchProcessor(wait=0.1)
        pids = [as_is(rec) for rec in  ["1", "2", "3"]]
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called repeatedly before callback is called repeatedly
        # this illustrates the "don't" of search_api.search.return
        assert captured.out == "lazy-1\nlazy-2\nlazy-3\n1\n2\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.2

    def test_get_single_pid(self):
        pids = get_pids('doi:10.5072/DAR/ATALUT')
        assert pids == ['doi:10.5072/DAR/ATALUT']

    def test_get_pids_from_file(self, tmp_path):
        with open(os.path.join(tmp_path, 'pids.txt'), 'w') as f:
            f.write('doi:10.5072/DAR/ATALUT\ndoi:10.17026/dans-xfg-s8q3\n')
            f.close()
            pids = get_pids(f.name)
            assert pids == ['doi:10.5072/DAR/ATALUT','doi:10.17026/dans-xfg-s8q3']
