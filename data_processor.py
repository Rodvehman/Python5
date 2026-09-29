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
            return all(isinstance(x,(int, float))
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
    def validate(self, data: any) -> bool:
        if isinstance(data, str) and len(data) > 0:
            return True
        elif isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, str) and len(x) > 0 for x in data)
        else:
            return False

        def ingest(self, data: Any) -> None:
            if not self.validade(data):
                return
            
