# E-Commerce Data Streaming Analytics
This repository features a production-grade data pipeline leveraging Confluent Kafka for real-time collection of e-commerce clickstream and user activity data. It integrates Azure Data Lake Storage Gen2 (ADLS Gen2) and Databricks for efficient data storage, cleansing, and transformation. With Kafka producers managing user profiles, delivery statuses, orders, and clickstreams, this project highlights proficiency in practical streaming applications and real-time analytics workflows.

## Video Documentation
Link: [YouTube](https://youtu.be/2NZTa6HGbE4)

## **Project Overview**
This project demonstrates a real-time data streaming pipeline where data is ingested into **Confluent Kafka topics** using Python, stored in **Azure Data Lake Storage Gen2 (ADLS Gen2)** via a Kafka sink connector, and analyzed in two different scenarios:

1. **Real-Time Analytics**: Perform real-time analytics directly from Kafka topics.
2. **Near Real-Time Analytics**: Perform analytics by fetching data from ADLS Gen2 into Azure Databricks for further processing and transformation.

### **Use Cases**
- Real-time clickstream analysis.

- Near real-time order analysis for e-commerce.

- User behavior tracking, orders, clickstream and delivery status updates.

---

## Pipeline Workflow
![Pipeline Workflow](https://github.com/tahir007malik/ecommerceDataStreamingAnalytics/blob/main/Docs/ecommerceDataStreamingAnalytics-workflow.png)

## **Architecture**

1. **Data Producers**: Python scripts generate synthetic data for topics like `clickstream`, `orders`, `user-profiles`, and `delivery-status`.

2. **Confluent Kafka**: Acts as the message broker to store and distribute real-time data streams.

3. **Azure Blob Storage Sink Connector**: Transfers data from Kafka topics to ADLS Gen2.

4. **Azure Data Lake Storage Gen2 (ADLS Gen2)**: Acts as a centralized storage layer for raw and processed data.

5. **Azure Databricks**: Fetches data from ADLS Gen2, performs data transformations, and generates analytics-ready datasets.

6. **Analysis**: Conduct real-time analytics using Kafka topics or near real-time analytics using transformed data in ADLS Gen2.

---

## **Getting Started**

### **Prerequisites**

1. **Confluent Kafka**
   - A running Confluent Cloud instance or a local Kafka setup.
   - Confluent Cloud API key and secret.
2. **Azure Resources**
   - Azure Storage Account with Data Lake Gen2 enabled.
   - Azure Databricks Workspace.
3. **Python Packages**
   - `confluent-kafka`
   - `json`
   - `datetime`
4. **Kafka Connect**
   - Azure Blob Storage Sink Connector.

### **Environment Setup**
#### **Install Python Dependencies**
```bash
pip install confluent-kafka
```

#### **Kafka Topics**
Create the following Kafka topics:
- `clickstream`
- `orders`
- `user-profiles`
- `delivery-status`

![Kafka Topics](https://github.com/tahir007malik/ecommerceDataStreamingAnalytics/blob/main/Docs/ecommerceDataStreamingAnalytics-topic.png)
---

## **Step 1: Producing Data to Kafka Topics**

![Kafka Producer Code](https://github.com/tahir007malik/ecommerceDataStreamingAnalytics/blob/main/Docs/ecommerceDataStreamingAnalytics-producer.png)

### **Clickstream Data Producer**
Python script to generate synthetic clickstream data: /Producer/clickstream.py

Repeat similar producer scripts for `orders`, `user-profiles`, and `delivery-status`.

---

## **Step 2: Using Kafka Connect to Load Data into ADLS Gen2**

### **Azure Blob Storage Sink Connector Configuration**
- Configure the sink connector to move data from Kafka to ADLS Gen2:

![Kafka Connector](https://github.com/tahir007malik/ecommerceDataStreamingAnalytics/blob/main/Docs/ecommerceDataStreamingAnalytics-connector.png)

Deploy this configuration for each topic (`orders`, `user-profiles`, and `delivery-status`).

---

## **Step 3: Real-Time vs Near Real-Time Analytics**

### **Scenario 1: Real-Time Analytics from Kafka**
1. Consume data directly from Kafka topics using a Databricks data streaming functionality.
2. Process and analyze the data in real-time.

### **Scenario 2: Near Real-Time Analytics with Databricks**
1. Fetch raw data from ADLS Gen2 into Databricks.
2. Apply transformations, filtering, and sorting.
3. Store the processed data back in ADLS Gen2 for downstream analytics.

#### Databricks Notebook Example: /Notebook/clickstream-notebook.ipynb, etc.

## **Conclusion**
This project showcases:
- **Real-Time Streaming**: Processing data directly from Kafka topics.
- **Near Real-Time Analytics**: Processing stored data from ADLS Gen2 via Databricks.
- **Scalable Architecture**: Leveraging Kafka, Azure, and Databricks for large-scale data streaming and analytics.

Feel free to expand this pipeline further by adding machine learning models, dashboards, or other analytics layers!
