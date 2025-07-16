import os
from pathlib import Path

def main():
    pwd = Path().cwd()  # get the current working directory as a `Path` object
    subdirs = [str(subdir) for subdir in pwd.iterdir() if subdir.is_dir()]  # filter out anything that's not a directory (files, etc.) and cast to string for easier handling

    # create `__init__.py` files inside each subdirectory
    for subdir in subdirs:
        initfile = os.path.join(subdir, '__init__.py')  # use os.path.join() instead of `Path()` because we only want the path as a string here
        with open(initfile, 'w') as f:
            print('created', initfile)
            pass  # you could put code in here to make any changes or modifications you need inside each `__init__.py` file. For example: `f.write("""import my_module""")`

if __name__ == "__main__":
    main()