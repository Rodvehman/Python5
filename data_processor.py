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
        last_processed: tuple[int, str] = self._processed_data.pop(0)
        return last_processed


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, (int, float))
                       and not isinstance(x, bool) for x in data)
        else:
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
        else:
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
            and "message" in item
            and isinstance(item["log_level"], str)
            and len(item["log_level"]) > 0
            and isinstance(item["message"], str)
            and len(item["message"]) > 0
        )

    def validate(self, data: Any) -> bool:
        if self.is_valid_log(data):
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(self.is_valid_log(x) for x in data)
        else:
            return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            formated_log = f"[{item['log_level']}]: {item['message']}"
            self._processed_data.append((self._rank_counter, formated_log))
            self._rank_counter += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    value1 = 42
    print(f"Trying to validate '{value1}': {numeric.validate(value1)}")
    value2 = 'Hello'
    print(f"Trying to validate '{value2}': {numeric.validate(value2)}")
    text = TextProcessor()
    value3 = 'foo'
    print(
        f"Test invalid ingestion of string '{value3}'"
        f" without prior validation:")
    try:
        numeric.ingest(value3)
        raise ValueError("Improper numeric data")
    except ValueError as e:
        print(f"Got exception: {e}")
    list_values = [1, 2, 3, 4, 5]
    print(f"Processing data: {list_values}")
    numeric.ingest(list_values)
    print("Extrating 3 values...")
    for _ in range(3):
        rank, val = numeric.output()
        print(f"Numeric value {rank}: {val}")
    print()
    print("Trying Text Processor...")
    text_value = TextProcessor()
    print(f"Trying to validate input '42': {text_value.validate(42)}")
    text_list = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_list}")
    try:
        text_value.ingest(text_list)
    except ValueError as e:
        print(f"Got exception: {e}")
    for _ in range(1):
        rank, val = text_value.output()
        print("Extracting value...")
        print(f"Text value {rank}: {val}")
    print()
    print("Testing Log Processor...")
    log = LogProcessor()
    value_log = 'Helo'
    print(f"Trying to validate input '{value_log}': {log.validate(value_log)}")
    try:
        log.ingest(value_log)
    except ValueError as e:
        print(f"Got exception: {e}")
    log_list = [
        {'log_level': 'NOTICE', 'message': 'Connection to server'},
        {'log_level': 'ERROR', 'message': 'Unauthorized access!!'}
        ]
    log.ingest(log_list)
    print(f"{log_list}")
    for _ in range(2):
        rank, val = log.output()
        print(f"Log entry {rank}: {val}")
