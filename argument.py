import argparse

# Use nargs to specify how many arguments an option should take.
ap = argparse.ArgumentParser()
ap.add_argument('-a', nargs=2)
ap.add_argument('-b', nargs=3)
ap.add_argument('-c', nargs=1)

# An illustration of how access the arguments.
opts = ap.parse_args('-a A1 A2 -b B1 B2 B3 -c C1'.split())

print(opts)
print(opts.a)
print(opts.b)
print(opts.c)

# To require that at least one option be supplied (-a, -b, or -c)
# you have to write your own logic. For example:
opts = ap.parse_args([])
if not any([opts.a, opts.b, opts.c]):
    ap.print_usage()
    quit()

print("This won't run.")
