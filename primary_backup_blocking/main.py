import pathlib
import main

ROOT_DIR:str = pathlib.Path(__file__).parent.parent.resolve()

print(ROOT_DIR)