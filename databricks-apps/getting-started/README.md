# Getting Started with Databricks Apps and Positron

This repository contains a simple example for building a Databricks App using Positron. It demonstrates how to create a basic web application that can be deployed on Databricks while leveraging Positron's unique capabilities as a a data science specific IDE.

## Prerequisites
- A Databricks account with access to the workspace.
- Basic understanding of Python and web development concepts.
- Access to [Positron](https://positron.posit.co/), either locally or within Posit Workbench.

## Data
This example uses a sample synthetic dataset that is included in [data](/data). The dataset is a parquet file containing synthetic sales data:

| Field | Type | Description |
|-------|------|-------------|
| `transaction_id` | string | Unique identifier for each sales transaction |
| `date` | date | Date when the transaction occurred |
| `product` | string | Name or description of the product sold |
| `quantity` | int64 | Number of units sold in the transaction |
| `unit_price` | float64 | Price per individual unit of the product |
| `customer_id` | string | Unique identifier for the customer |
| `customer_name` | string | Full name of the customer |
| `customer_email` | string | Email address of the customer |
| `customer_segment` | string | Customer categorization (e.g., enterprise, SMB, etc.) |
| `region` | string | Geographic region where the sale occurred |
| `sales_channel` | string | Channel through which the sale was made (e.g., online, retail, etc.) |
| `salesperson` | string | Name of the salesperson who handled the transaction |
| `salesperson_tier` | string | Performance tier or level of the salesperson |
| `discount_percent` | float64 | Percentage discount applied to the transaction |
| `total_amount` | float64 | Final total amount for the transaction after discounts |

## Positron Assistant
[Positron Assistant](https://positron.posit.co/assistant.html) is an AI tool within Positron that can provide assistance with a variety of tasks. See [documentation](https://positron.posit.co/assistant.html#step-2-configure-language-model-providers) for how to configure Positron Assistant with an Anthropic API key. In this example, we supplement Positron Assistant with an [`llms.txt`](llms.txt) file that provides additional context about Databricks Apps. This file is used to enhance Positron Assistant's understanding of Databricks Apps, allowing it to provide more relevant and accurate assistance for developing and deploying applications to Databricks.

## Workflow
The workflow used to build this example application is as follows:
1. **Data Exploration**: Use Positron and Positron Assistant to explore the dataset, understand its structure, and identify key insights. This is represented in [`eda.qmd`](eda.qmd).
2. **Application Development**: Develop a Shiny application for the dataset using Positron and Positron Assistant. In this case, we prompted Positron Assistant to generate a Shiny application based on the analysis done in the previous step. The resulting application is at [`app/app.py](app/app.py).
3. **Local Testing**: Test the Shiny application locally using Positron's built-in capabilities. This allows you to ensure that the application behaves as expected before deploying it to Databricks.
4. **Deployment**: Deploy the Shiny application to Databricks using guidance from Positron Assistant. With the application open, we can ask Positron Assistant to generate the necessary deployment commands.
