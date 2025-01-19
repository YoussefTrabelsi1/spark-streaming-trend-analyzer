# Real-Time Trend Analyzer 🚀

[![PySpark](https://img.shields.io/badge/Apache%20Spark-Streaming-brightgreen)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-blue)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

---

## 📄 Overview

A real-time data streaming system using PySpark Structured Streaming and Kafka. Processes live data to detect trends via windowed aggregations, demonstrating scalable and distributed data processing.

---

## ⚙️ Tech Stack

- **PySpark Structured Streaming**: Distributed data processing engine.
- **Apache Kafka**: Stream data source for real-time ingestion.
- **Python**: For scripting and implementation.

---

## 🏗️ Architecture

1. **Data Source**: Simulated IoT logs or social media feeds.
2. **Kafka**: Streams incoming data to topics.
3. **PySpark**: Processes Kafka streams with window-based aggregations.
4. **Output**: Trends detected and stored in a database or displayed on a console.

```text
  Simulated Data  -->  Kafka Topics  -->  PySpark Processing  -->  Output (Console/DB)
```

---

## ⚡ Quick Setup

### Prerequisites
- Python 3.x
- Apache Spark 3.x
- Apache Kafka
- Java (for Spark)

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/YoussefTrabelsi1/spark-streaming-trend-analyzer.git
   cd spark-streaming-trend-analyzer
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Kafka services:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```

4. Update Kafka configuration in `config.json`.

5. Run the streaming job:
   ```bash
   spark-submit streaming_job.py
   ```

---

## 🚀 Future Enhancements

- Add real-time dashboards (e.g., Grafana or Streamlit).
- Integrate anomaly detection algorithms.
- Expand support for multiple Kafka topics.

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
