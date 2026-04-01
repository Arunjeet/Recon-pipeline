#!/bin/bash
set -euo pipefail

echo "Fetching credentials" 

#python xerobootstrap.py

echo "updating tables from Xero API" #python main.py pnl 2026-01-01 2026-01-31 --periods 5 --timeframe MONTH #python main.py tb 2025-12-31 #Updating CI tables...

python main.py manualjournal

python main.py journal

python main.py account #python main.py journal

echo "Running main_sheet..."
python shielded_expense.py


echo "All scripts completed successfully."
