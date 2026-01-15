#!/bin/bash
echo "=== Stock Platform - Conda Setup ==="

# start conda
source /home/twi-dev/anaconda3/etc/profile.d/conda.sh
echo "0. Starte Conda"
conda activate

# Environment erstellen
echo "1. Erstelle Conda Environment..."
conda create -n stock-platform 

# Environment aktivieren
echo "2. Aktiviere Environment..."
conda activate stock-platform

# Install Spyder
echo "2.1 Installiere Spyder"
conda install anaconda::spyder

# Pakete installieren
echo "3. Installiere Pakete..."
conda install -c conda-forge psycopg2 sqlalchemy pandas numpy requests python-dotenv spyder-kernels -y

# Pip-Pakete
echo "4. Installiere Pip-Pakete..."
pip install timescaledb
pip install -U massive

# Verification
echo "5. Teste Installation..."
python -c "import psycopg2, sqlalchemy, pandas; print('✅ Alle Pakete verfügbar!')"

echo ""
echo "=== Setup abgeschlossen! ==="
echo "Starte Spyder mit: conda activate stock-platform && spyder"
