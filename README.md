# Projeto Integração com EDA

This project is a data integration pipeline designed to process product data from a CSV file and send it to an external API. The pipeline follows the ETL (Extract, Transform, Load) methodology and adheres to good programming practices, such as modularity, logging, and error handling.

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-yellow)

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Key Components](#key-components)
- [Data Processing Workflow](#data-processing-workflow)
- [Screenshots](#screenshots)
- [Best Practices](#best-practices)
- [License](#license)

## Overview

The project processes raw product data stored in a CSV file, transforms it into a structured format, and sends it to an external API in batches. The pipeline includes the following steps:

1. **Data Extraction**: Reads raw data from a CSV file.  
2. **Data Transformation**: Cleans, formats, and enriches the data.  
3. **Batch Creation**: Divides the processed data into manageable batches.  
4. **Data Loading**: Sends the batches to the external API.

## Project Structure

```plaintext
projeto-integracao/
├── .env                      # Environment variables
├── .gitignore                # Git ignore rules
├── main.py                   # Entry point of the application
├── processing.log            # Log file for monitoring
├── README.md                 # Documentation
├── requirements.txt          # Python dependencies
├── batches/                  # JSON files for batch processing
│   ├── batch_1.json
│   └── ...
├── data/
│   └── raw-bronze/
│       └── items.csv         # Raw data source
├── src/
│   ├── api_client.py         # Handles API interactions
│   ├── processor.py          # Contains data processing logic
│   ├── utils.py              # Utility functions for data formatting
│   └── __pycache__/          # Compiled Python files
```

## Installation

Clone the repository:

```bash
git clone https://github.com/Luish1927/projeto-integracao.git
cd projeto-integracao
```

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

To execute the pipeline, run the following command:

```bash
python main.py
```

This will process the data, create batches, and send them to the API.

## Key Components

### `main.py`

The entry point of the application. It orchestrates the pipeline by:

- Initializing the `ProductAPIClient` with the path to the input CSV file.
- Calling methods to process data, create batches, and send them to the API.

### `src/processor.py`

Defines the `ProductDataProcessor` class, which handles data processing:

- `load_and_process()`: Reads the CSV file, formats and enriches the data, and prepares it for batch creation.
- `create_batches()`: Divides the processed data into JSON batches of 1,000 records each.

### `src/api_client.py`

Defines the `ProductAPIClient` class, which extends `ProductDataProcessor`:

- `send_batches()`: Sends the JSON batches to the external API and logs the responses.

### `src/utils.py`

Provides utility functions for data formatting and validation:

- `measure_parser()`: Extracts dimensions and weight from product names.
- `format_to_ean()`: Validates and formats barcodes to EAN standards.
- `promo_price_formater()`: Formats promotional prices.
- `internal_code_formater()`: Converts internal codes to strings.
- `infer_unit_type()`: Infers the unit type of the product.
- `float_to_string()`: Converts the barcode from float to string.
- `price_formater()`: Converts the price of the product to float.
- `stock_formater()`: Converts the stock format to float.
- `ptbr_to_iso_format()`: Converts the date from pt-BR format to ISO format.
- `process_promo_dates()`: Processes the "Data termino promocao" to extract the dates and save them into ISO format.

## Data Processing Workflow

### Data Loading

- The `load_and_process()` method reads the raw CSV file using pandas.
- It extracts and formats fields such as barcodes, prices, and promotional dates.

### Data Transformation

- The `measure_parser()` function extracts dimensions and weight from product names to create the `weight`, `length`, `width`, and `height` columns.
- Barcodes are validated and formatted using `format_to_ean()`.

### Batch Creation

- The `create_batches()` method divides the processed data into JSON files, each containing up to 1,000 records.

### API Integration

- The `send_batches()` method sends the JSON batches to the external API and logs the results.

## 🔎 API Response Inconsistency (Documentation vs Actual Behavior)

During the implementation of this project, I encountered an inconsistency between the API documentation and the actual response received from the API. Although the payload sent strictly follows the format described in the official documentation, the response from the API does not behave as expected.

### ✅ Payload Sent (According to Documentation)

The following is an example of the data sent to the API, which aligns with the structure and field types described in the documentation:

![Request Payload](screenshot/request-payload.png)

---

### 📥 Expected Response (As per Documentation)

According to the documentation, a successful submission should result in a response like this:

![Expected Response](screenshot/expected-response.png)

---

### ⚠️ Actual Response Received

Despite using valid and complete data, the API returned the following response indicating that **no items were processed**, even though the request was successful (`http_status: 200`):

![Unexpected Response](screenshot/unexpected-response.png)

This indicates that the data is being received, but not registered or updated as expected — which contradicts the documented behavior.

---

### 📌 Reference from API Sample

Here’s a snippet from the documentation showing the expected structure — which my payload **exactly matches**:

![Sample from Docs](screenshot/api-sample.png)

![Integration reference](screenshot/integration-reqs.png)

---

### 🧾 Final Notes

- The request follows the schema provided by the documentation.
- The server returns a `200 OK` status but does not process the data correctly.

This section is intended to **document and clarify the behavior mismatch** for future reference and to support any reviews or audits of this implementation.


## Best Practices

- **Modular Design**: Code is organized into separate modules for better readability and maintainability.
- **Logging**: All key operations are logged to `processing.log` for debugging and monitoring.
- **Error Handling**: API interactions and data processing include error handling to ensure robustness.
- **Environment Variables**: Sensitive information, such as API keys, is stored in the `.env` file and loaded securely.
- **Data Validation**: Utility functions validate and format data to ensure consistency.