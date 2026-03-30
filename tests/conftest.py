import sys
from pathlib import Path

# Ajoute la racine du projet au path Python pour que "from src.x import y" fonctionne
sys.path.insert(0, str(Path(__file__).parents[1]))
