#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._processed_data: list[tuple[int, str]] = []
        self._rank_counter: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._processed_data:
            raise IndexError("No data available to output")
        return self._processed_data.pop(0)


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in data
            )
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._processed_data.append((self._rank_counter, str(item)))
            self._rank_counter += 1


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str) and len(data) > 0:
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, str) and len(x) > 0 for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._processed_data.append((self._rank_counter, item))
            self._rank_counter += 1


class LogProcessor(DataProcessor):

    def is_valid_log(self, item: Any) -> bool:
        return (
            isinstance(item, dict)
            and "log_level" in item
            and ("log_message" in item or "message" in item)
            and isinstance(item["log_level"], str)
            and len(item["log_level"]) > 0
        )

    def validate(self, data: Any) -> bool:
        if self.is_valid_log(data):
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(self.is_valid_log(x) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            msg_key = "log_message" if "log_message" in item else "message"
            formatted_log = f"{item['log_level']}: {item[msg_key]}"
            self._processed_data.append((self._rank_counter, formatted_log))
            self._rank_counter += 1


class DataStream:

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        name = proc.__class__.__name__.replace("Processor", " Processor")
        print(f"Registering {name}")
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled: bool = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            if not handled:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{item}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total = proc._rank_counter
            remaining: int = len(proc._processed_data)
            print(
                f"{name}: total {total} items processed, remaining "
                f"{remaining} on processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    stream.print_processors_stats()
