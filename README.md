<h1 align="center">⚡ Real-Time Electricity & Emissions Streaming Pipeline ⚡</h1>


<p align="center">
  <span style="font-size:20px;">📊 Data Engineering &nbsp;|&nbsp; 📡 MQTT Stream Processing &nbsp;|&nbsp; 🗺 Geospatial Analytics</span>
</p>

<p align="center">
  <img src="assets/geospatial-dashboard.png" width="800" alt="Geospatial Dashboard Preview"/>
</p>

---

## Project Overview
This project engineers an **end-to-end Real-Time Data Pipeline** designed to monitor and analyse the **Australian National Electricity Market (NEM)**. By synthesising high-frequency telemetry from the **Open Electricity API**, the system simulates a continuous, unbounded data stream representing a nationwide IoT sensor network.

The core mission is to demonstrate a production-grade transition from raw, high-dimensional research data to actionable strategic intelligence. Through a decoupled architecture using **MQTT message brokering**, the pipeline ensures that 5-minute-interval power generation and CO2 emission metrics are delivered with sub-second latency, providing stakeholders with an intuitive, geospatial interface for immediate grid-load and carbon-footprint inference.

> **Translating complex research into scalable AI solutions, ensuring that high-dimensional data is transformed into intuitive, real-time tools for strategic decision-making.**

---

# System Architecture

The system follows a modular **Producer–Consumer pattern** to ensure clear separation of concerns, scalability, and reliable data persistence.

![System Architecture](geospatial-dashboard-architecture.png)

<h2>🏗 Architecture Overview</h2>

<table>
<tr>
<td width="50%" valign="top">

<h3>📥 Ingestion Layer (REST API)</h3>

<ul>
<li>Automated polling of the NEM network for facility-level telemetry</li>
<li>Structured data normalization and validation</li>
<li>Continuous, production-grade data acquisition</li>
</ul>

</td>

<td width="50%" valign="top">

<h3>🗄 Materialization Layer (Caching Engine)</h3>

<ul>
<li>Specialized caching strategy to mitigate API rate-limiting</li>
<li>Maintains a localized <b>Source of Truth</b> for historical trend analysis</li>
<li>Optimized for high-frequency read operations</li>
</ul>

</td>
</tr>

<tr>
<td width="50%" valign="top">

<h3>📡 Transport Layer (MQTT)</h3>

<ul>
<li>High-throughput message broker</li>
<li>Ordered, event-driven communication</li>
<li>Real-time streaming from data source to visualisation engine</li>
</ul>

</td>

<td width="50%" valign="top">

<h3>🖥️ Presentation Layer (Streamlit)</h3>

<ul>
<li>Reactive geospatial dashboard</li>
<li>Real-time MQTT subscription</li>
<li>Stateful marker management for dynamic visual updates</li>
</ul>

</td>
</tr>
</table>

---

# Key Features & Visual Highlights

| Feature | Technical Implementation | Professional Value |
|----------|--------------------------|--------------------|
| **Unbounded Streaming** | Throttled Publishing (0.1s) & API Looping | Simulates real-world industrial IoT environments |
| **Geospatial Intelligence** | Leaflet/Folium Integration | Provides spatial context to generation intensity |
| **Dynamic Inferences** | Real-time Payload Switching | Instant toggle between Power (MW) and CO₂ (tCO₂e) |
| **System Resilience** | Intelligent Rate-Limit Handling | Ensures 99.9% uptime despite strict API constraints |

---

# Design Principles

- **Modularity** – Each layer operates independently  
- **Scalability** – Horizontally extensible transport and caching  
- **Resilience** – Built-in retry & backoff mechanisms  
- **Observability** – Structured logging & performance metrics  
- **Real-Time First** – Designed for low-latency streaming environments  

---

## 📦 Installation & Setup

### 1. Clone the repository
```bash
git clone [https://github.com/venkatbadri/nem-streaming-analytics.git](https://github.com/venkatbadri/nem-streaming-analytics.git)
cd nem-streaming-analytics
```
### 2. Install Dependencies
```bash
pip install -r requirements.txt
```
### 3. Configure Environment
* **API Access:** Obtain an API Key from Open Electricity.

* **MQTT Broker:** Set up your credentials for a broker of choice (e.g., HiveMQ, Mosquitto, or a Local Broker).

### 4. Run the Pipeline
Start the Producer (Tasks 1-3): 
```bash
python producer.py
```

Launch the Dashboard: 
```bash
streamlit run app.py
```

## 📊 Professional Insights & Data Engineering Focus
* **Data Integrity:** Implemented linear interpolation for missing 5-minute interval values to ensure stream continuity and maintain the accuracy of predictive models.

* **Latency Optimization:** Reduced external overhead by architecting a local materialization layer, effectively eliminating redundant API calls and optimizing resource consumption.

* **Architectural Scalability:** Utilized a decoupled MQTT design, allowing for seamless multi-subscriber consumption (e.g., simultaneous database logging and real-time dashboard visualization).
