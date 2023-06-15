# module for result writer classes
import csv
import json
import typing

import yaml


# Abstract base class for the result writers
class ResultWriter:
    def write(self, result: dict, is_first: bool):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


# Function that write a result in the form of a dictionary to a text stream
class JsonResultWriter(ResultWriter):
    def __init__(self, out_stream: typing.TextIO):
        self.first_result_written = False
        self.out_stream = out_stream

    def write(self, result: dict, is_first: bool):
        if is_first:
            self.out_stream.write("[")
            self.first_result_written = True
        else:
            self.out_stream.write(", ")
        self.out_stream.write(json.dumps(result))
        self.out_stream.flush()

    def close(self):
        if not self.first_result_written:
            self.out_stream.write("[")
        self.out_stream.write("]")


class YamlResultWriter(ResultWriter):
    def __init__(self, out_stream: typing.TextIO):
        self.out_stream = out_stream

    def write(self, result: dict, is_first: bool):
        self.out_stream.write(yaml.dump(result))

    def close(self):
        pass


class CsvResultWriter(ResultWriter):
    def __init__(self, headers: list, out_stream: typing.TextIO):
        self.out_stream = out_stream
        self.headers = headers
        self.csv_writer = csv.DictWriter(out_stream, fieldnames=headers, lineterminator="\n")
        self.csv_writer.writeheader()

    def write(self, result: dict, is_first: bool):
        if len(result.keys()) > 0:
            if set(result.keys()) != set(self.headers):
                raise ValueError("Result keys do not match headers")
            else:
                self.csv_writer.writerow(result)

    def close(self):
        pass
