from setuptools import setup, Extension, Feature

# original version
VERSION="0.5"

# get current version
with open('brenda/version.py') as f:
    exec(f.read())

paracurl = Feature(
    'Paracurl',
    ext_modules = [Extension("paracurl", ["paracurl/paracurl.c"], libraries=['curl'])]
)

setup(name = "Brenda",
      version = VERSION,
      packages = ['brenda'],
      scripts = ['brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node'],
      features = {'paracurl': paracurl},
      author = "Sen Haerens",
      author_email = "sen@senhaerens.be",
      description = "Render farm tool for cloud computing services",
)
