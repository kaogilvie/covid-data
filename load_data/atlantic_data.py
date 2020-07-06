

def addapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class ATLDataLoader(load_utils.DataLoader):
    def __init__(self, local=True):
        super().__init__(schema='atlantic')

if __name__ == "__main__":

    # deal with numpy interpolation
    register_adapter(numpy.int64, addapt_numpy_int64)

    args = argparse.ArgumentParser()
    args.add_argument('-t', '--table', help='table to refresh')
    args.add_argument('-f', '--filename', help='path to filename')
    argz = args.parse_args()

    table = argz.table
    filename = args.filename
    local = False
    if table is None:
        local = True
        table = f'daily_states'
        filename = f'daily_states.csv'

    nyt = ATLDataLoader(False)
