# Emeralds - GTFS RT Data Viewer

## Overview
Emeralds is a Streamlit-based application designed to view and download GTFS RT (General Transit Feed Specification Real-Time) data from various providers. It allows users to select feeds, dates, and hours to fetch and visualize transit data.

## Features
- **Provider Selection**: Choose from multiple transit data providers.
- **Feed Type Selection**: Select specific feed types (e.g., Vehicle Position, Trip Update, Alert).
- **Date and Hour Selection**: Use a calendar input to select dates and hours for data retrieval.
- **Data Download**: Export data in Parquet, CSV, or JSON formats.
- **Code Generation**: Download Python code to fetch data locally.
- **Data Visualization**: Visualize vehicle positions on a map using Plotly and PyDeck.
- **Replay Transit Data**: Animate transit data with adjustable replay speed.

## Installation

### Prerequisites
- Python 3.12 or higher
- Pip

### Required Packages
Install the dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Usage
1. Run the application:
   ```bash
   streamlit run gui.py
   ```
2. Select a provider, feed type, and date.
3. Fetch and visualize data.
4. Download data or generated code for local use.

## Project Structure
- `gui.py`: Main application file.
- `fetch.py`: Contains functions for fetching GTFS RT data.
- `requirements.txt`: Lists required Python packages.

## Dependencies
- **Streamlit**: For building the interactive GUI.
- **PyArrow**: For handling Parquet and CSV data.
- **Plotly**: For map-based visualizations.
- **PyDeck**: For animated transit data replay.
- **Streamlit Calendar Input**: For date selection.

## License
This project is licensed under the MIT License.

## Contact
For questions or contributions, please contact the developer via GitHub.