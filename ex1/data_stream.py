from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:

        self.stream_id = stream_id
        self.processed_count = 0
        self.error_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        return [item for item in data_batch if item is not None]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Environmental Data"
        self.total_readings = 0
        self.avg_temp = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            readings = len(data_batch)
            self.processed_count += readings
            self.total_readings += readings

            temps = []
            for item in data_batch:
                if isinstance(item, dict) and "temp" in item:
                    temps.append(item["temp"])
                elif isinstance(item, str) and "temp:" in item:
                    temp_val = float(item.split("temp:")[1].split(",")[0])
                    temps.append(temp_val)

            if temps:
                self.avg_temp = sum(temps) / len(temps)

            result = f"Sensor analysis: {readings} readings processed"
            if self.avg_temp > 0:
                result += f", avg temp: {self.avg_temp}°C"

            return result
        except Exception as e:
            self.error_count += 1
            return f"Error processing sensor data: {e}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        if criteria.lower() == "critical":
            filtered: List[Any] = []
            for item in data_batch:
                try:
                    if isinstance(item, str) and "temp:" in item:
                        temp_value = float(item.split("temp:", 1)[1]
                                           .split(",")[0])
                        if temp_value >= 30.0:
                            filtered.append(item)
                    elif isinstance(item, dict) and "temp" in item:
                        if float(item["temp"]) >= 30.0:
                            filtered.append(item)
                except Exception:
                    continue
            return filtered
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["stream_type"] = self.stream_type
        stats["total_readings"] = self.total_readings
        stats["avg_temp"] = self.avg_temp
        return stats


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Financial Data"
        self.total_operations = 0
        self.net_flow = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            operations = len(data_batch)
            self.processed_count += operations
            self.total_operations += operations

            flow = 0
            for item in data_batch:
                if isinstance(item, dict):
                    if "buy" in item:
                        flow -= item["buy"]
                    if "sell" in item:
                        flow += item["sell"]
                elif isinstance(item, str):
                    if "buy:" in item:
                        val = int(item.split("buy:")[1])
                        flow -= val
                    elif "sell:" in item:
                        val = int(item.split("sell:")[1])
                        flow += val

            self.net_flow += flow

            result = f"Transaction analysis: {operations} operations"
            if flow != 0:
                sign = "+" if flow > 0 else ""
                result += f", net flow: {sign}{flow} units"

            return result
        except Exception as e:
            self.error_count += 1
            return f"Error processing transaction data: {e}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        if criteria.lower() == "large":
            filtered: List[Any] = []
            for item in data_batch:
                try:
                    if isinstance(item, str) and ":" in item:
                        amount = float(item.split(":", 1)[1])
                        if amount >= 120.0:
                            filtered.append(item)
                    elif isinstance(item, dict):
                        for val in item.values():
                            if float(val) >= 120:
                                filtered.append(item)
                                break
                except Exception:
                    continue
            return filtered
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["stream_type"] = self.stream_type
        stats["total_operations"] = self.total_operations
        stats["net_flow"] = self.net_flow
        return stats


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "System Events"
        self.total_events = 0
        self.errors_detected = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            events = len(data_batch)
            self.processed_count += events
            self.total_events += events

            errors = 0
            for item in data_batch:
                item_str = str(item).lower()
                if "error" in item_str or "fail" in item_str:
                    errors += 1

            self.errors_detected += errors

            result = f"Event analysis: {events} events"
            if errors > 0:
                result += (
                    f", {errors} error detected"
                    if errors == 1
                    else f", {errors} errors detected"
                )
            else:
                result += " processed"

            return result
        except Exception as e:
            self.error_count += 1
            return f"Error processing event data: {e}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        if criteria.lower() == "error":
            return [item for item in data_batch if str(item) == "error"]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["stream_type"] = self.stream_type
        stats["total_events"] = self.total_events
        stats["errors_detected"] = self.errors_detected
        return stats


class StreamProcessor:

    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        results = []
        for stream, batch in zip(self.streams, data_batches):
            result = stream.process_batch(batch)
            results.append(result)
        return results


def demo_streams() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = ["temp:22.5, humidity:65, pressure:1013"]
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    print(sensor.process_batch(sensor_data))
    print()

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(
         f"Stream ID: {transaction.stream_id}, "
         f"Type: {transaction.stream_type}")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    print("Processing transaction batch: [buy:100, sell:150, buy:75]")
    print(transaction.process_batch(trans_data))
    print()

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    event_data = ["login", "error", "logout"]
    print("Processing event batch: [login, error, logout]")
    print(event.process_batch(event_data))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    processor.add_stream(SensorStream("SENSOR_002"))
    processor.add_stream(TransactionStream("TRANS_002"))
    processor.add_stream(EventStream("EVENT_002"))

    test_batches = [
        ["temp:31.0", "temp:35.0"],
        ["buy:50", "sell:200", "buy:20", "sell:60"],
        ["login", "logout", "error"],
    ]

    print("Batch 1 Results:")
    results = processor.process_all(test_batches)
    print(f"- Sensor data:{results[0].split(':')[1].split(',')[0]}")
    print(
         f"- Transaction data:{results[1].split(':')[1].split(',')[0]}"
         f"processed"
         )
    print(f"- Event data:{results[2].split(':')[1].split(',')[0]} processed")
    print()

    sensor_filtered = sensor.filter_data(test_batches[0], "critical")
    transaction_filtered = transaction.filter_data(test_batches[1], "large")
    event_filtered = event.filter_data(test_batches[2], "error")
    print("Stream filtering active: High-priority data only")
    print(
        f"Filtered results: "
        f"{len(sensor_filtered)} critical sensor alerts, "
        f"{len(transaction_filtered)} large transaction, "
        f"{len(event_filtered)} error events"
        )
    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    demo_streams()
