This application will allow a tennis captain to compare teams to build the perfect lineup


## Directory Structure
tennis_lineup/
│── data/                   # Stores collected data (JSON, CSV, etc.)
│   ├── raw/                # Unprocessed API responses
│   ├── processed/          # Cleaned and structured data
│── src/                    # Source code
│   ├── api/                # API interaction scripts
│   │   ├── utr_api.py      # Handles UTR API requests
│   ├── processing/         # Data processing scripts
│   │   ├── player_stats.py # Parses and analyzes player stats
│   ├── lineup/             # Logic for lineup recommendations
│   │   ├── lineup_generator.py # Determines matchups based on stats
│   ├── main.py             # Entry point for the program
│── tests/                  # Unit tests for scripts
│── notebooks/              # Jupyter notebooks for exploration
│── config.py               # Configuration settings (API keys, credentials)
│── requirements.txt        # Python dependencies
│── README.md               # Project documentation

