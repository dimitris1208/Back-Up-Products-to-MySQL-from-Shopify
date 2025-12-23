# Sync Script Documentation

## Overview
The `sync.py` script is a data synchronization utility that automatically fetches all products from a Shopify store and persists them to a MySQL database hosted on PythonAnywhere.

## Purpose
This script enables seamless integration between your Shopify platform and a MySQL database by:
- Retrieving complete product catalogs from Shopify
- Storing product data in a structured MySQL database
- Automating the synchronization process

## Features
- **Automated Product Fetching**: Connects to Shopify API to retrieve all products
- **Database Storage**: Saves product information to MySQL on PythonAnywhere
- **Batch Processing**: Efficiently handles large product catalogs

## Limitations
⚠️ **Note**: This script does **not** track variants levels. Product inventory data is not synchronized or updated during the sync process.

## Requirements
- Shopify API credentials
- MySQL database on PythonAnywhere
- Python environment with required dependencies

## Usage
Simply run the script to initiate the synchronization