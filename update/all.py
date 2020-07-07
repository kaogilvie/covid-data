from update import nytimes
from update import atlantic

def update_all():
    nytimes.run_update()
    atlantic.run_update()

if __name__ == "__main__":
    update_all()
