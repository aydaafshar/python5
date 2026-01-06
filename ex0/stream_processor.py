from abc import ABC, abstractmethod
from typing import Any, List

class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"{result}"

class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data , list):
            raise TypeError("NumericProcessor expects a list")
        for x in data:
            if not isinstance(x, (int, float)):
                raise TypeError("NumericProcessor expects numeric values only")
        return True
    
    def process(self, data: Any) -> str:
        self.validate(data)
        count = len(data)
        total = 0
        for x in data:
            total += x
        avg = total / count if count > 0 else 0
        return self.format_output(
            f"Processed {count} numeric values, sum={total}, avg={avg}"
        )

class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            raise TypeError("TextProcessor expects a string")
        return True
    
    def process(self, data: Any) -> str:
        self.validate(data)
        char_count = len(data)
        word_count = len(data.split())
        return self.format_output(
            f"Processed text: {char_count} characters, {word_count} words"
        )

class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            raise TypeError("LogProcessor expects a string")
        if ":" not in data:
            raise ValueError("LogProcessor expects LEVEL: message")
        return True

    def process(self, data: Any) -> str:
        self.validate(data)
        parts = data.split(":", 1)
        level = parts[0].upper()
        message= parts[1]

        tag = level
        if level == "ERROR":
            tag = "ALERT"

        return self.format_output(
            f"[{tag}] {level} level detected:{message}"
        )

def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    try:
        numeric = NumericProcessor()
        print("Initializing Numeric Processor...")
        print("Processing data: [1, 2, 3, 4, 5]")
        if numeric.validate([1, 2, 3, 4, 5]):
            print("Validation: Numeric data verified")
        print(f"{numeric.process([1, 2, 3, 4, 5])}\n")

        text = TextProcessor()
        print("Initializing Text Processor...")
        print('Processing data: "Hello Nexus World"')
        if text.validate("Hello Nexus World"):
            print("Validation: Text data verified")
        print(f"{text.process('Hello Nexus World')}\n")
        

        log = LogProcessor()
        print("Initializing Log Processor...")
        print('Processing data: "ERROR: Connection timeout"')
        if log.validate("ERROR: Connection timeout"):
            print("Validation: Log entry verified")
        print(f"{log.process('ERROR: Connection timeout')}\n")

        print("=== Polymorphic Processing Demo ===")
        print("Processing multiple data types through same interface...")

        processors: List[DataProcessor] = [
            NumericProcessor(),
            TextProcessor(),
            LogProcessor(),
        ]

        data_list: List[Any] = [
            [1, 2, 3],
            "Hello World!",
            "INFO: System ready",
        ]

        i=1
        for processor, data in zip(processors, data_list):
            try:
                result = processor.process(data)
                print(f"Result {i}: {result}")
            except Exception as exc:
                print(f"Result {i}: Error - {exc}")
            i += 1

        print("\nFoundation systems online. Nexus ready for advanced streams.")


    except Exception as exc:
        print(f"Fatal error: {exc}")


if __name__ == "__main__":
    main()


        
        